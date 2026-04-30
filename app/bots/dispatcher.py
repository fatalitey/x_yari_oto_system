"""Aday gönderileri yöneticiye Telegram üzerinden iletir."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from openai import OpenAI
from sqlalchemy.orm import Session
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.core.config import Settings
from app.db.models import Post, PostStatus
from app.db.repositories import PostRepository

logger = logging.getLogger(__name__)


def _kb(post_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Onayla", callback_data=f"a:{post_id}"),
                InlineKeyboardButton("Reddet", callback_data=f"r:{post_id}"),
            ]
        ]
    )


def _media_send_kind(media_path: str | None) -> str | None:
    if not media_path:
        return None
    ext = Path(media_path).suffix.lower()
    if ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return "photo"
    if ext in {".mp4", ".webm", ".mov", ".mkv", ".mpeg", ".m4v"}:
        return "video"
    return "document"


def _clip(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


def _translate_text_to_turkish(settings: Settings, text: str) -> str | None:
    """
    Sadece ekip önizlemesi için anlık çeviri.
    DB'ye yazılmaz, yayın metni olarak kullanılmaz.
    """
    if not settings.openai_api_key or not text.strip():
        return None
    try:
        client = OpenAI(api_key=settings.openai_api_key)
        resp = client.chat.completions.create(
            model=settings.openai_model,
            messages=[
                {
                    "role": "system",
                    "content": "Translate the given text to Turkish naturally. Keep meaning, keep concise.",
                },
                {"role": "user", "content": text},
            ],
            temperature=0.1,
        )
        out = (resp.choices[0].message.content or "").strip()
        return out or None
    except Exception as e:  # noqa: BLE001
        logger.warning("TR önizleme çevirisi alınamadı: %s", e)
        return None


async def _build_preview_text(settings: Settings, p: Post) -> str:
    original = (p.original_text or "").strip()
    tr = await asyncio.to_thread(_translate_text_to_turkish, settings, original[:3000])
    parts = [str(p.source_label), "", "Orijinal metin:", _clip(original or "(boş)", 2600)]
    if tr:
        parts.extend(["", "TR (sadece ekip önizlemesi):", _clip(tr, 1800)])
    return "\n".join(parts)


async def dispatch_candidates(session: Session, settings: Settings, limit: int = 5) -> int:
    if not settings.telegram_bot_token or settings.telegram_approval_chat_id is None:
        logger.warning("Bot token veya onay sohbeti (TELEGRAM_APPROVAL_CHAT_ID) eksik; onay gönderimi atlandı.")
        return 0
    from telegram import Bot

    bot = Bot(settings.telegram_bot_token)
    posts = PostRepository(session)
    rows_all = posts.list_candidates_for_dispatch(
        max_message_age_hours=settings.post_max_age_hours,
        limit=max(limit * 20, 50),
    )
    rows: list[Post] = []
    per_channel_counts: dict[str, int] = {}
    per_channel_limit = int(settings.dispatch_max_per_channel)
    for row in rows_all:
        cid = str(row.telegram_chat_id or "")
        used = per_channel_counts.get(cid, 0)
        if used >= per_channel_limit:
            continue
        per_channel_counts[cid] = used + 1
        rows.append(row)
        if len(rows) >= limit:
            break
    if not rows:
        logger.info(
            "Onay gönderilecek aday yok: havuzdan_gelen=%s istenen_ust=%s kanal_basina=%s",
            len(rows_all),
            limit,
            per_channel_limit,
        )
    sent = 0
    for idx, p in enumerate(rows, start=1):
        if await _send_one(bot, settings, session, p):
            sent += 1
        # Telegram tarafında burst timeout/rate-limit riskini düşürmek için kısa throttle.
        if idx < len(rows):
            await asyncio.sleep(0.9)
    session.commit()
    return sent


async def _send_one(bot, settings: Settings, session: Session, p: Post) -> bool:
    posts = PostRepository(session)
    preview_text = await _build_preview_text(settings, p)
    chat = settings.telegram_approval_chat_id
    try:
        kind = _media_send_kind(p.media_path)
        if p.media_path and kind and Path(p.media_path).is_file():
            with open(p.media_path, "rb") as fh:
                if kind == "photo":
                    await bot.send_photo(
                        chat_id=chat,
                        photo=fh,
                        caption=_clip(preview_text, 1024),
                        reply_markup=_kb(p.id),
                    )
                elif kind == "video":
                    await bot.send_video(
                        chat_id=chat,
                        video=fh,
                        caption=_clip(preview_text, 1024),
                        reply_markup=_kb(p.id),
                    )
                else:
                    await bot.send_document(
                        chat_id=chat,
                        document=fh,
                        filename=Path(p.media_path).name,
                        caption=_clip(preview_text, 1024),
                        reply_markup=_kb(p.id),
                    )
        else:
            await bot.send_message(
                chat_id=chat,
                text=_clip(preview_text, 4096),
                reply_markup=_kb(p.id),
            )
        posts.set_status(p.id, PostStatus.awaiting_approval.value)
        logger.info("Onay mesajı gönderildi post_id=%s", p.id)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("Onay mesajı gönderilemedi post_id=%s: %s", p.id, e)
        return False
