from __future__ import annotations

import html
import json
import logging
from datetime import timedelta
from pathlib import Path

from sqlalchemy.orm import Session

from app.ai.rewriter import rewrite_for_x
from app.core.config import Settings
from app.db.models import PostStatus
from app.db.repositories import PostRepository, PublishQueueRepository, utcnow

logger = logging.getLogger(__name__)


def _export_local_draft(
    *,
    settings: Settings,
    post_id: int,
    source_label: str,
    original_text: str | None,
    rewritten_text: str,
    media_path: str | None,
) -> Path:
    """
    X yayını kapalıyken yerel inceleme paketi üretir.
    """
    out_dir = settings.resolve_draft_export_root()
    payload = {
        "post_id": post_id,
        "source_label": source_label,
        "rewritten_text_for_x": rewritten_text,
        "original_text": original_text or "",
        "media_path": media_path or "",
        "publish_enabled": settings.publish_enabled,
    }
    json_path = out_dir / f"post_{post_id}.json"
    txt_path = out_dir / f"post_{post_id}.txt"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text(rewritten_text.strip() + "\n", encoding="utf-8")
    return json_path


def approve_post(session: Session, settings: Settings, post_id: int) -> tuple[str, str | None]:
    posts = PostRepository(session)
    queue = PublishQueueRepository(session)
    post = posts.get(post_id)
    if not post:
        return "Kayıt bulunamadı.", None
    if not posts.claim_for_approval(post_id):
        return "Bu gönderi zaten işlendi veya onay beklemiyor.", None
    session.commit()
    post = posts.get(post_id)
    if not post:
        return "Kayıt bulunamadı.", None

    text = (post.original_text or "").strip()
    if not text:
        posts.set_status(post_id, PostStatus.failed.value)
        session.commit()
        return "Metin boş, OpenAI atlandı.", None
    try:
        rewritten = rewrite_for_x(settings, text)
    except Exception as e:  # noqa: BLE001
        logger.exception("OpenAI hatası")
        post.status = PostStatus.failed.value
        session.commit()
        return f"OpenAI hatası: {e}", None

    posts.set_rewritten(post_id, rewritten)
    if settings.publish_enabled:
        when = utcnow() + timedelta(seconds=5)
        if not queue.has_pending_for_post(post_id):
            queue.enqueue(post_id, when)
        result_msg = "Onaylandı, kuyruğa alındı."
    else:
        posts.set_status(post_id, PostStatus.approved.value)
        draft_path = _export_local_draft(
            settings=settings,
            post_id=post.id,
            source_label=post.source_label,
            original_text=post.original_text,
            rewritten_text=rewritten,
            media_path=post.media_path,
        )
        result_msg = f"Onaylandı, GPT önizleme hazır. Taslak dosyası: {draft_path}"
    session.commit()
    return result_msg, rewritten


def reject_post(session: Session, post_id: int) -> str:
    posts = PostRepository(session)
    post = posts.get(post_id)
    if not post:
        return "Kayıt bulunamadı."
    posts.set_status(post_id, PostStatus.rejected.value)
    session.commit()
    return "Reddedildi."


def format_publish_queue_list(session: Session, *, limit: int = 15) -> str:
    q = PublishQueueRepository(session)
    posts = PostRepository(session)
    rows = q.list_pending_ordered(limit)
    if not rows:
        return "Yayın kuyruğu boş."
    lines: list[str] = ["<b>Yayın kuyruğu</b> (eskiden yeniye):"]
    for i, row in enumerate(rows, start=1):
        p = posts.get(row.post_id)
        snip = ""
        if p:
            raw = (p.rewritten_text or p.original_text or "").replace("\n", " ").strip()
            snip = html.escape(raw[:72] + ("…" if len(raw) > 72 else ""))
        when = row.scheduled_at.isoformat() if row.scheduled_at else "?"
        lines.append(f"{i}. post #{row.post_id} • {when}\n<pre>{snip}</pre>")
    return "\n".join(lines)


def dequeue_publish_queue_position(session: Session, position_1based: int) -> str:
    if position_1based < 1:
        return "Geçersiz sıra numarası."
    q = PublishQueueRepository(session)
    posts = PostRepository(session)
    n = len(q.list_pending_ordered(200))
    pid = q.delete_pending_at_position(position_1based)
    if pid is None:
        return f"Sırada yeterli kayıt yok (toplam bekleyen: {n})."
    post = posts.get(pid)
    if post and post.status == PostStatus.queued.value:
        posts.set_status(pid, PostStatus.approved.value)
    session.commit()
    return f"Sıra {position_1based} kuyruktan çıkarıldı (post #{pid})."
