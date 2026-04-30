"""
İki (veya daha fazla) uzak düğümde aynı işleri tekrarlamayı önlemek için kira tabanlı lider seçimi.

SQLite üzerinde `system_state` satırı güncellenir. PostgreSQL'e geçişte aynı mantık çalışır.

Önemli: Her düğümün aynı DATABASE_URL'e yazabilmesi gerekir (ortak disk veya merkezi sunucu).
Yerel ayrı SQLite dosyalarıyla iki makine aynı anda 'lider' olur; bu yapılandırma desteklenmez.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from app.db.repositories import SystemStateRepository

logger = logging.getLogger(__name__)

LEADER_KEY = "scheduler_leader"


@dataclass
class LeaderInfo:
    node_id: str
    lease_until: datetime


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LeaderElection:
    def __init__(self, session: Session, node_id: str, lease_seconds: int) -> None:
        self._repo = SystemStateRepository(session)
        self._node_id = node_id
        self._lease_seconds = lease_seconds

    def try_acquire_or_renew(self) -> bool:
        now = _utcnow()
        raw = self._repo.get_value(LEADER_KEY)
        new_lease = now + timedelta(seconds=self._lease_seconds)

        if raw:
            try:
                data = json.loads(raw)
                current_id = data.get("node_id")
                lease_until = datetime.fromisoformat(data["lease_until"])
                if lease_until.tzinfo is None:
                    lease_until = lease_until.replace(tzinfo=timezone.utc)
            except (json.JSONDecodeError, KeyError, ValueError):
                current_id, lease_until = None, now - timedelta(seconds=1)

            if lease_until > now and current_id != self._node_id:
                logger.debug("Lider başka düğüm: %s", current_id)
                return False

        payload = json.dumps(
            {
                "node_id": self._node_id,
                "lease_until": new_lease.isoformat(),
            }
        )
        self._repo.upsert(LEADER_KEY, payload)
        logger.debug("Lider kirası: %s -> %s", self._node_id, new_lease.isoformat())
        return True

    def release_if_holder(self) -> None:
        raw = self._repo.get_value(LEADER_KEY)
        if not raw:
            return
        try:
            data = json.loads(raw)
            if data.get("node_id") == self._node_id:
                self._repo.upsert(LEADER_KEY, json.dumps({"node_id": "", "lease_until": _utcnow().isoformat()}))
        except (json.JSONDecodeError, KeyError):
            pass
