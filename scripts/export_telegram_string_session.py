"""
TELEGRAM_SESSION_STRING üretmek için bir kerelik çalıştırın.

Önkoşul: https://my.telegram.org adresinden API ID ve API Hash alın.

Kullanım (proje kökünden):
  python scripts/export_telegram_string_session.py

Çıktıdaki satırı .env içindeki TELEGRAM_SESSION_STRING= değerine yapıştırın.
"""

from __future__ import annotations

import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    api_id = int(input("API ID (my.telegram.org): ").strip())
    api_hash = input("API Hash: ").strip()
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()
    s = client.session.save()
    await client.disconnect()
    print("\n.env için aşağıdaki satırı kopyalayın:\n")
    print(f"TELEGRAM_SESSION_STRING={s}\n")


if __name__ == "__main__":
    asyncio.run(main())
