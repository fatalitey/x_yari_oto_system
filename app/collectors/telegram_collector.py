"""
Telethon ile izlenen sohbetlerden son mesajları çeker.
Pencere: son 2 saat (POST_MAX_AGE_HOURS), dedup sonrası aday havuzuna alınır.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)

from app.core.config import Settings
from app.core.storage import LocalMediaStorage
from app.db.models import PostStatus
from app.db.repositories import PostRepository
from app.filters.post_filter import PostFilterService

logger = logging.getLogger(__name__)

_LOCKED_STATUSES = {
    PostStatus.awaiting_approval.value,
    PostStatus.processing_approval.value,
    PostStatus.approved.value,
    PostStatus.queued.value,
    PostStatus.published.value,
    PostStatus.rejected.value,
    PostStatus.hold.value,
}


def _msg_created_at(message: object) -> datetime | None:
    if message.date:
        d = message.date
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    return None


def _pick_media_filename(message: object) -> str | None:
    if not message.media:
        return None
    mid = message.id
    if isinstance(message.media, MessageMediaPhoto):
        return f"tg_{mid}.jpg"
    if isinstance(message.media, MessageMediaDocument) and message.media.document:
        doc = message.media.document
        mime = (doc.mime_type or "").lower()
        for a in doc.attributes or []:
            if isinstance(a, DocumentAttributeVideo):
                return f"tg_{mid}.mp4"
        if mime.startswith("video/"):
            return f"tg_{mid}.mp4"
        if mime.startswith("image/") and "gif" not in mime:
            return f"tg_{mid}.jpg"
        for a in doc.attributes or []:
            if isinstance(a, DocumentAttributeFilename) and a.file_name:
                safe = re.sub(r"[^\w.\-]", "_", Path(a.file_name).name)[:100]
                return f"tg_{mid}_{safe}"
        return f"tg_{mid}.bin"
    return f"tg_{mid}.bin"


def _reaction_total(message: object) -> int:
    reactions = getattr(message, "reactions", None)
    if reactions and getattr(reactions, "results", None):
        return sum(getattr(r, "count", 0) or 0 for r in reactions.results)
    return 0


async def _resolve_effective_payload(client, entity, message: object) -> tuple[object, str, int, object | None]:
    """
    Referans/reply mesajı varsa ana mesaj + referans metnini tek payload olarak birleştirir.
    Dönüş: (ana_mesaj, birlesik_metin, birlesik_reaksiyon, medya_kaynagi_mesaji)
    """
    primary = message
    primary_text = (getattr(message, "message", "") or "").strip()
    merged_reactions = _reaction_total(message)

    reply_to = getattr(message, "reply_to", None)
    reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None)
    if reply_to_msg_id:
        try:
            parent = await client.get_messages(entity, ids=reply_to_msg_id)
        except Exception:  # noqa: BLE001
            parent = None
        if parent and getattr(parent, "id", None):
            primary = parent
            parent_text = (getattr(parent, "message", "") or "").strip()
            if parent_text and primary_text and parent_text != primary_text:
                primary_text = f"{parent_text}\n\n--- Referans mesaj ---\n{primary_text}"
            elif parent_text:
                primary_text = parent_text
            merged_reactions = _reaction_total(parent) + _reaction_total(message)

    media_source = primary if getattr(primary, "media", None) else message if getattr(message, "media", None) else None
    return primary, primary_text, merged_reactions, media_source


async def collect_once(session: Session, settings: Settings, media_root: Path) -> int:
    if not settings.telegram_api_id or not settings.telegram_api_hash or not settings.telegram_session_string:
        logger.warning("Telegram toplayıcı: API kimlikleri veya oturum eksik, atlanıyor.")
        return 0
    chats = settings.monitor_chat_list
    if not chats:
        logger.warning("TELEGRAM_MONITOR_CHATS boş.")
        return 0

    storage = LocalMediaStorage(media_root)
    posts_repo = PostRepository(session)
    filt = PostFilterService(session, settings)
    max_msg_h = float(settings.post_max_age_hours)
    now = datetime.now(timezone.utc)
    max_age_delta = timedelta(hours=max_msg_h)
    fetch_limit = int(settings.telegram_fetch_limit)

    client = TelegramClient(
        StringSession(settings.telegram_session_string),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    processed = 0
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Telegram oturumu yetkili değil.")
        await client.disconnect()
        return 0

    try:
        for spec in chats:
            entity = await client.get_entity(spec)
            label = getattr(entity, "title", None) or getattr(entity, "username", None) or str(spec)
            seen_primary_ids: set[int] = set()
            async for message in client.iter_messages(entity, limit=fetch_limit):
                if message is None or not getattr(message, "id", None):
                    continue
                # Pencere hesaplaması her zaman görülen asıl mesajın zamanına göre yapılır.
                created = _msg_created_at(message)
                if created and (now - created) > max_age_delta:
                    # iter_messages yeniden eskiye gider; buradan sonrası daha da eski.
                    break

                primary, text, reaction_count, media_message = await _resolve_effective_payload(client, entity, message)
                primary_id = int(getattr(primary, "id", message.id))
                if primary_id in seen_primary_ids:
                    continue
                seen_primary_ids.add(primary_id)

                fp = storage.fingerprint_text(text) if text.strip() else ""
                status, reason = filt.classify_for_pool(
                    telegram_chat_id=str(entity.id),
                    telegram_message_id=str(primary_id),
                    created_at=created,
                    reaction_count=reaction_count,
                    fingerprint=fp,
                    text=text or None,
                )
                if status == PostStatus.dropped.value:
                    logger.debug("Mesaj elendi chat=%s msg=%s: %s", entity.id, message.id, reason)

                media_path: str | None = None
                if status == PostStatus.candidate.value and media_message and getattr(media_message, "media", None):
                    msg_for_media = media_message
                    fname = _pick_media_filename(msg_for_media) or f"tg_{primary_id}.bin"
                    dest = storage.path_for(str(entity.id), str(primary_id), fname)
                    if not storage.exists(dest):
                        try:
                            await client.download_media(msg_for_media, file=str(dest))
                            media_path = str(dest)
                        except Exception as e:  # noqa: BLE001
                            logger.warning("Medya indirilemedi: %s", e)
                    else:
                        media_path = str(dest)

                row = posts_repo.add_or_update_seen(
                    chat_id=str(entity.id),
                    message_id=str(primary_id),
                    source_label=str(label),
                    original_text=text or None,
                    media_path=media_path,
                    created_at=created or now,
                    reaction_count=reaction_count,
                    fingerprint_hash=fp,
                )
                # Bir post onay/yayın akışına girdiyse restart sonrası tekrar candidate'e düşmesin.
                if row.status not in _LOCKED_STATUSES:
                    row.status = status
                processed += 1
        session.commit()
    finally:
        await client.disconnect()

    logger.info("Telegram toplama tamamlandı, işlenen mesaj: %s", processed)
    return processed
