"""
Yerel dosya depolama. İleride S3 vb. için aynı arayüz genişletilebilir.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalMediaStorage:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, chat_id: str, message_id: str, filename: str) -> Path:
        safe = f"{chat_id}_{message_id}_{filename}".replace("/", "_")
        return self.root / safe

    def exists(self, path: Path) -> bool:
        return path.is_file()

    @staticmethod
    def fingerprint_text(text: str) -> str:
        normalized = " ".join(text.lower().split())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def prune_older_than_hours(self, hours: float) -> int:
        """
        Media klasöründe son değiştirilme zamanı bu eşikten eski dosyaları siler.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max(hours, 1.0))
        removed = 0
        for p in self.root.iterdir():
            if not p.is_file():
                continue
            try:
                mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    p.unlink(missing_ok=True)
                    removed += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("Medya temizliği: dosya silinemedi (%s): %s", p, e)
        return removed
