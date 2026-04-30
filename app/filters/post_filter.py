from __future__ import annotations

from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import PostStatus
from app.db.repositories import PostRepository


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PostFilterService:
    """Havuz kuralları: son pencere + dedup (sabit tepki eşiği yok)."""

    def __init__(self, session: Session, settings: Settings) -> None:
        self._posts = PostRepository(session)
        self._settings = settings

    def _dedup_window_hours(self) -> float:
        return max(float(self._settings.pool_max_fetched_age_hours), 1.0)

    def classify_for_pool(
        self,
        *,
        telegram_chat_id: str,
        telegram_message_id: str,
        created_at: datetime | None,
        reaction_count: int,
        fingerprint: str,
        text: str | None,
    ) -> tuple[str, str]:
        """
        Dönüş: (PostStatus değeri, kısa gerekçe).
        candidate = pencere içinde ve dedup temiz.
        dropped = elenmiş.
        """
        now = utcnow()
        win = self._dedup_window_hours()
        fps = self._posts.list_recent_fingerprints(
            hours=win,
            exclude_chat_id=telegram_chat_id,
            exclude_message_id=telegram_message_id,
        )
        if fingerprint and fingerprint in fps:
            return PostStatus.dropped.value, "tam kopya parmak izi"

        if text and text.strip():
            recent = self._posts.list_recent_texts(
                [
                    PostStatus.raw.value,
                    PostStatus.candidate.value,
                    PostStatus.awaiting_approval.value,
                    PostStatus.queued.value,
                    PostStatus.published.value,
                ],
                hours=win,
                exclude_chat_id=telegram_chat_id,
                exclude_message_id=telegram_message_id,
            )
            for other in recent:
                ratio = SequenceMatcher(None, text, other).ratio()
                if ratio >= self._settings.near_duplicate_similarity:
                    return PostStatus.dropped.value, f"yakın kopya ({ratio:.2f})"

        max_msg_h = float(self._settings.post_max_age_hours)
        msg_age: timedelta | None = None
        if created_at:
            c = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
            msg_age = now - c

        if msg_age is not None and msg_age > timedelta(hours=max_msg_h):
            return PostStatus.dropped.value, "mesaj çok eski"

        return PostStatus.candidate.value, "aday"
