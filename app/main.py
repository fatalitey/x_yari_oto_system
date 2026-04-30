"""
Ana giriş: zamanlayıcı + (isteğe bağlı) onay botu thread'i.

Varsayılan: SQLite bu cihazda; cihaz değişince veri sıfırlanır (dosyayı taşımazsanız).
Ortak DB + çok süreç için LEADER_ELECTION_ENABLED=true kullanın.
Yedek düğümde APPROVAL_BOT_ENABLED=false ile çift bot polling önlenir.
"""

from __future__ import annotations

import logging
import threading
import time

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler

from app.bots import approval_bot as approval_bot_module
from app.core.config import get_settings
from app.core.logging_setup import configure_logging
from app.db.session import init_db
from app.scheduler.jobs import job_dispatch_approval, job_fetch_telegram, register_jobs

logger = logging.getLogger(__name__)


def _run_bot_thread() -> None:
    while True:
        try:
            approval_bot_module.main()
        except Exception:  # noqa: BLE001
            logger.exception("Onay botu thread hatası; 5sn sonra yeniden denenecek")
            time.sleep(5)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()

    if settings.standalone_approval_bot:
        logger.info("Yalnızca onay botu modu.")
        approval_bot_module.main()
        return

    # SQLite eşzamanlı yazma kilidine düşmemek için scheduler job'larını sırayla koştur.
    scheduler = BackgroundScheduler(
        timezone=settings.scheduler_timezone or "UTC",
        executors={"default": ThreadPoolExecutor(max_workers=1)},
    )
    register_jobs(scheduler, settings)
    scheduler.start()
    logger.info(
        "Zamanlayıcı başladı (node=%s). Ctrl+C ile durdurun.",
        settings.node_id,
    )

    if settings.run_initial_fetch_dispatch:
        logger.info("Başlangıç turu: Telegram çekimi ve onay gönderimi tetikleniyor.")
        job_fetch_telegram()
        job_dispatch_approval()

    if settings.approval_bot_enabled:
        bot_thread = threading.Thread(target=_run_bot_thread, daemon=True, name="approval-bot")
        bot_thread.start()
    else:
        logger.info("Onay botu bu düğümde devre dışı (APPROVAL_BOT_ENABLED=false).")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Kapatılıyor...")
    finally:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
