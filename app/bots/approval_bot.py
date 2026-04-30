"""
Onay / red callback'leri ve yayın kuyruğu komutları (/sira).

Ayrı süreç: python -m app.bots.approval_bot
Ana süreçle birlikte: main içinde arka plan thread olarak başlatılabilir.
"""

from __future__ import annotations

import asyncio
import html
import logging
import re

from telegram import Update
from telegram.error import NetworkError, TimedOut
from telegram.request import HTTPXRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, Defaults

from app.bots import approval_service
from app.core.config import get_settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

_ACTION_RE = re.compile(r"^([ar]):(\d+)$")


async def _reply_with_retry(message, text: str, *, parse_mode: str = "HTML", retries: int = 2) -> None:
    for attempt in range(retries + 1):
        try:
            await message.reply_text(text[:4096], parse_mode=parse_mode)
            return
        except (TimedOut, NetworkError) as e:
            if attempt >= retries:
                logger.warning("Mesaj gönderimi başarısız (retry tükendi): %s", e)
                return
            await asyncio.sleep(1.5 * (attempt + 1))


async def _on_bot_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, (TimedOut, NetworkError)):
        logger.warning("Telegram ağ timeout hatası (işlem sürdürülecek): %s", err)
        return
    logger.exception("Onay botu handler hatası", exc_info=err)


def _actor_name(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "Bilinmeyen"
    if u.username:
        return f"@{u.username}"
    return u.full_name


def _approval_chat_ok(update: Update) -> bool:
    settings = get_settings()
    cid = update.effective_chat.id if update.effective_chat else None
    if settings.telegram_approval_chat_id is None or cid is None:
        return False
    return int(cid) == int(settings.telegram_approval_chat_id)


async def _mark_rejected_on_message(update: Update) -> None:
    q = update.callback_query
    if not q or not q.message:
        return
    actor = html.escape(_actor_name(update))
    badge = f"\n\n❌ <b>Reddedildi</b> • {actor}"
    try:
        if q.message.caption is not None:
            new_caption = (q.message.caption + badge)[:1024]
            await q.edit_message_caption(caption=new_caption, parse_mode="HTML", reply_markup=None)
            return
        if q.message.text is not None:
            new_text = (q.message.text + badge)[:4096]
            await q.edit_message_text(text=new_text, parse_mode="HTML", reply_markup=None)
            return
    except Exception:  # noqa: BLE001
        logger.warning("Reddedildi işareti mesaja yazılamadı.")


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    m = _ACTION_RE.match(q.data)
    if not m:
        await q.answer("Geçersiz düğme")
        return
    action, pid_s = m.group(1), m.group(2)
    post_id = int(pid_s)
    settings = get_settings()

    def work() -> tuple[str, str | None]:
        with SessionLocal() as session:
            if action == "a":
                return approval_service.approve_post(session, settings, post_id)
            return approval_service.reject_post(session, post_id), None

    msg, preview = await asyncio.to_thread(work)

    await q.answer()
    if action == "r":
        await _mark_rejected_on_message(update)
    else:
        await q.edit_message_reply_markup(reply_markup=None)
    if q.message:
        await _reply_with_retry(q.message, msg, parse_mode="HTML")
        if action == "a" and preview:
            preview_text = (
                "<b>GPT Önizleme (X için)</b>\n"
                f"<pre>{preview[:3900]}</pre>"
            )
            await _reply_with_retry(q.message, preview_text, parse_mode="HTML")


async def cmd_sira(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if not _approval_chat_ok(update):
        return
    args = context.args or []

    def work() -> str:
        with SessionLocal() as session:
            if not args:
                return approval_service.format_publish_queue_list(session, limit=15)
            if len(args) >= 2 and args[0].isdigit() and args[1].lower() == "cikar":
                return approval_service.dequeue_publish_queue_position(session, int(args[0]))
            return "Kullanım: /sira  veya  /sira 6 cikar"

    msg = await asyncio.to_thread(work)
    await _reply_with_retry(update.message, msg, parse_mode="HTML")


def build_application() -> Application:
    settings = get_settings()
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN gerekli.")
    app = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .request(
            HTTPXRequest(
                connect_timeout=20.0,
                read_timeout=40.0,
                write_timeout=40.0,
                pool_timeout=20.0,
            )
        )
        .defaults(Defaults(parse_mode="HTML"))
        .build()
    )
    app.add_handler(CallbackQueryHandler(on_callback, pattern=r"^[ar]:\d+$"))
    app.add_handler(CommandHandler("sira", cmd_sira))
    app.add_error_handler(_on_bot_error)
    return app


def main() -> None:
    from app.core.logging_setup import configure_logging
    from app.db.session import init_db

    configure_logging(get_settings().log_level)
    init_db()
    app = build_application()
    logger.info("Onay botu dinlemede.")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        timeout=60,
        # approval bot ana thread dışında çalıştırılabildiği için signal handler kapat.
        stop_signals=None,
    )


if __name__ == "__main__":
    main()
