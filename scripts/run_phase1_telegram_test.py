"""
Faz 1 duman testi: Telethon ile kaynakları çeker → filtre → onay sohbetine (grup) gönderir.
Yayın kuyruğu / X yok; sadece toplama + gruba iletim.

Örnek (.env dolu, proje kökünden):
  python scripts/run_phase1_telegram_test.py

Son 1 saat + en az 100 tepki + kanal başına son 150 mesaj (varsayılanlar):
  python scripts/run_phase1_telegram_test.py --post-max-age-hours 1 --min-reactions 100 --fetch-limit 150 --dispatch 15
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    sys.path.insert(0, str(ROOT))

    parser = argparse.ArgumentParser(description="Telegram toplama + onay grubuna gönderim (test).")
    parser.add_argument(
        "--post-max-age-hours",
        type=float,
        default=1.0,
        help="Aday için mesaj en fazla bu kadar saat önce (varsayılan: 1)",
    )
    parser.add_argument(
        "--min-reactions",
        type=int,
        default=100,
        help="Aday için minimum tepki/emote (varsayılan: 100)",
    )
    parser.add_argument(
        "--dispatch",
        type=int,
        default=15,
        help="Gruba en fazla kaç aday mesajı",
    )
    parser.add_argument(
        "--fetch-limit",
        type=int,
        default=150,
        help="Kanal başına taranacak son mesaj sayısı",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

    os.environ["POST_MAX_AGE_HOURS"] = str(args.post_max_age_hours)
    os.environ["POST_MIN_REACTIONS"] = str(args.min_reactions)
    os.environ["TELEGRAM_FETCH_LIMIT"] = str(args.fetch_limit)

    from app.core.config import clear_settings_cache, get_settings

    clear_settings_cache()

    from app.bots.dispatcher import dispatch_candidates
    from app.collectors.telegram_collector import collect_once
    from app.core.logging_setup import configure_logging
    from app.db.session import SessionLocal, init_db
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()

    media = settings.resolve_media_root(project_root=ROOT)

    async def run() -> None:
        with SessionLocal() as session:
            n = await collect_once(session, settings, media)
            print(f"Toplanan / güncellenen mesaj kaydı: {n}")
        with SessionLocal() as session:
            sent = await dispatch_candidates(session, settings, limit=args.dispatch)
            print(f"Gruba gönderilen onay mesajı: {sent}")
            if sent == 0:
                print(
                    "(İpucu) Aday yoksa: kaynakta son mesajlarda 100+ tepki + mesaj yaşı "
                    f"≤ {args.post_max_age_hours} saat birlikte sağlanmıyor olabilir; "
                    "veya .env TELEGRAM_MONITOR_CHATS / oturum kontrol edin."
                )

    asyncio.run(run())


if __name__ == "__main__":
    main()
