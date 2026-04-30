from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Sequence

from sqlalchemy import delete, or_, select, update
from sqlalchemy.orm import Session

from app.db.models import Post, PostStatus, PublishQueue, QueueStatus, SystemState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SystemStateRepository:
    def __init__(self, session: Session) -> None:
        self._s = session

    def get_value(self, key: str) -> str | None:
        row = self._s.get(SystemState, key)
        return row.value if row else None

    def upsert(self, key: str, value: str) -> None:
        row = self._s.get(SystemState, key)
        if row:
            row.value = value
            row.updated_at = utcnow()
        else:
            self._s.add(SystemState(key=key, value=value, updated_at=utcnow()))


class PostRepository:
    def __init__(self, session: Session) -> None:
        self._s = session

    def get_by_telegram_ids(self, chat_id: str, message_id: str) -> Post | None:
        stmt = select(Post).where(
            Post.telegram_chat_id == chat_id,
            Post.telegram_message_id == message_id,
        )
        return self._s.execute(stmt).scalar_one_or_none()

    def add_or_update_seen(
        self,
        *,
        chat_id: str,
        message_id: str,
        source_label: str,
        original_text: str | None,
        media_path: str | None,
        created_at: datetime | None,
        reaction_count: int,
        fingerprint_hash: str,
    ) -> Post:
        existing = self.get_by_telegram_ids(chat_id, message_id)
        if existing:
            existing.original_text = original_text or existing.original_text
            existing.media_path = media_path or existing.media_path
            existing.reaction_count = reaction_count
            existing.fingerprint_hash = fingerprint_hash
            existing.fetched_at = utcnow()
            return existing
        p = Post(
            source_label=source_label,
            telegram_chat_id=chat_id,
            telegram_message_id=message_id,
            original_text=original_text,
            media_path=media_path,
            created_at=created_at,
            reaction_count=reaction_count,
            fingerprint_hash=fingerprint_hash,
            status=PostStatus.raw.value,
        )
        self._s.add(p)
        self._s.flush()
        return p

    def delete_by_telegram_ids(self, chat_id: str, message_id: str) -> None:
        row = self.get_by_telegram_ids(chat_id, message_id)
        if row:
            self._s.delete(row)

    def list_recent_fingerprints(
        self,
        hours: float = 48.0,
        *,
        exclude_chat_id: str | None = None,
        exclude_message_id: str | None = None,
    ) -> set[str]:
        since = utcnow() - timedelta(hours=hours)
        stmt = select(Post.fingerprint_hash).where(
            Post.fetched_at >= since,
            Post.fingerprint_hash != "",
        )
        if exclude_chat_id is not None and exclude_message_id is not None:
            stmt = stmt.where(
                or_(
                    Post.telegram_chat_id != exclude_chat_id,
                    Post.telegram_message_id != exclude_message_id,
                )
            )
        return {row[0] for row in self._s.execute(stmt)}

    def list_recent_texts(
        self,
        statuses: Sequence[str],
        hours: float = 48.0,
        *,
        exclude_chat_id: str | None = None,
        exclude_message_id: str | None = None,
    ) -> list[str]:
        since = utcnow() - timedelta(hours=hours)
        stmt = select(Post.original_text).where(
            Post.fetched_at >= since,
            Post.status.in_(list(statuses)),
            Post.original_text.isnot(None),
        )
        if exclude_chat_id is not None and exclude_message_id is not None:
            stmt = stmt.where(
                or_(
                    Post.telegram_chat_id != exclude_chat_id,
                    Post.telegram_message_id != exclude_message_id,
                )
            )
        return [row[0] for row in self._s.execute(stmt) if row[0]]

    def list_by_status(self, status: str, limit: int = 100) -> list[Post]:
        stmt = select(Post).where(Post.status == status).order_by(Post.fetched_at.desc()).limit(limit)
        return list(self._s.scalars(stmt))

    def list_candidates_for_dispatch(
        self,
        *,
        max_message_age_hours: float,
        limit: int = 200,
    ) -> list[Post]:
        """
        Son N saatlik havuzdan (henüz onaya gitmemiş) en yüksek etkileşimli kayıtları getirir.
        """
        since = utcnow() - timedelta(hours=max_message_age_hours)
        eligible = (PostStatus.candidate.value,)
        stmt = (
            select(Post)
            .where(
                Post.created_at.isnot(None),
                Post.created_at >= since,
                Post.status.in_(eligible),
            )
            .order_by(Post.reaction_count.desc(), Post.created_at.desc(), Post.fetched_at.desc())
            .limit(limit)
        )
        return list(self._s.scalars(stmt))

    def set_status(self, post_id: int, status: str) -> None:
        self._s.execute(update(Post).where(Post.id == post_id).values(status=status))

    def claim_for_approval(self, post_id: int) -> bool:
        """
        Yarış durumunu engellemek için postu atomik olarak onay işleniyor durumuna çeker.
        """
        stmt = (
            update(Post)
            .where(
                Post.id == post_id,
                Post.status.in_([PostStatus.awaiting_approval.value, PostStatus.candidate.value]),
            )
            .values(status=PostStatus.processing_approval.value)
        )
        result = self._s.execute(stmt)
        return (result.rowcount or 0) == 1

    def set_rewritten(self, post_id: int, text: str) -> None:
        self._s.execute(
            update(Post).where(Post.id == post_id).values(rewritten_text=text, status=PostStatus.queued.value)
        )

    def get(self, post_id: int) -> Post | None:
        return self._s.get(Post, post_id)


class PublishQueueRepository:
    def __init__(self, session: Session) -> None:
        self._s = session

    def has_pending_for_post(self, post_id: int) -> bool:
        stmt = (
            select(PublishQueue.id)
            .where(
                PublishQueue.post_id == post_id,
                PublishQueue.status == QueueStatus.pending.value,
            )
            .limit(1)
        )
        return self._s.execute(stmt).scalar_one_or_none() is not None

    def enqueue(self, post_id: int, scheduled_at: datetime, priority: int = 0) -> PublishQueue:
        q = PublishQueue(post_id=post_id, scheduled_at=scheduled_at, priority=priority, status=QueueStatus.pending.value)
        self._s.add(q)
        self._s.flush()
        return q

    def next_due(self, now: datetime | None = None) -> PublishQueue | None:
        now = now or utcnow()
        stmt = (
            select(PublishQueue)
            .where(
                PublishQueue.status == QueueStatus.pending.value,
                PublishQueue.scheduled_at <= now,
            )
            .order_by(PublishQueue.scheduled_at.asc(), PublishQueue.priority.desc())
            .limit(1)
        )
        return self._s.execute(stmt).scalar_one_or_none()

    def mark(self, queue_id: int, status: str, error: str | None = None) -> None:
        vals: dict = {"status": status}
        if error is not None:
            vals["last_error"] = error
        self._s.execute(update(PublishQueue).where(PublishQueue.id == queue_id).values(**vals))

    def dequeue_pending_for_post(self, post_id: int) -> int:
        stmt = delete(PublishQueue).where(
            PublishQueue.post_id == post_id,
            PublishQueue.status == QueueStatus.pending.value,
        )
        result = self._s.execute(stmt)
        return result.rowcount or 0

    def list_pending_ordered(self, limit: int = 30) -> list[PublishQueue]:
        stmt = (
            select(PublishQueue)
            .where(PublishQueue.status == QueueStatus.pending.value)
            .order_by(PublishQueue.scheduled_at.asc(), PublishQueue.id.asc())
            .limit(limit)
        )
        return list(self._s.scalars(stmt))

    def delete_pending_at_position(self, position_1based: int) -> int | None:
        """1 tabanlı sıra; silinen satırın post_id döner."""
        rows = self.list_pending_ordered(position_1based)
        if position_1based < 1 or position_1based > len(rows):
            return None
        row = rows[position_1based - 1]
        pid = row.post_id
        self._s.delete(row)
        return pid


class CleanupRepository:
    """Eski satırları sil — operasyonel tampon mantığı."""

    def __init__(self, session: Session) -> None:
        self._s = session

    def purge_posts_fetched_before(self, max_fetched_age_hours: float) -> int:
        """Son alımdan bu kadar süre geçen tüm post kayıtlarını sil."""
        cutoff = utcnow() - timedelta(hours=max_fetched_age_hours)
        stmt = delete(Post).where(
            Post.fetched_at < cutoff,
        )
        result = self._s.execute(stmt)
        return result.rowcount or 0

    def purge_old_low_reaction_posts(self, max_message_age_hours: float, min_reactions: int) -> int:
        """Mesaj yaşı > eşik ve tepki < min ise sil (saatlik tazelense bile çöp kalmasın)."""
        cutoff = utcnow() - timedelta(hours=max_message_age_hours)
        disposable = (
            PostStatus.raw.value,
            PostStatus.dropped.value,
            PostStatus.candidate.value,
        )
        stmt = delete(Post).where(
            Post.reaction_count < min_reactions,
            Post.created_at.isnot(None),
            Post.created_at < cutoff,
            Post.status.in_(disposable),
        )
        result = self._s.execute(stmt)
        return result.rowcount or 0

    def purge_completed_queue(self, hours: int) -> int:
        cutoff = utcnow() - timedelta(hours=hours)
        stmt = delete(PublishQueue).where(
            PublishQueue.scheduled_at < cutoff,
            PublishQueue.status.in_([QueueStatus.done.value, QueueStatus.failed.value]),
        )
        result = self._s.execute(stmt)
        return result.rowcount or 0

    def purge_terminal_posts(self, hours: int) -> int:
        cutoff = utcnow() - timedelta(hours=hours)
        pending_posts = select(PublishQueue.post_id).where(PublishQueue.status == QueueStatus.pending.value)
        terminal = (
            PostStatus.dropped.value,
            PostStatus.rejected.value,
            PostStatus.published.value,
            PostStatus.failed.value,
            PostStatus.hold.value,
        )
        stmt = delete(Post).where(
            Post.fetched_at < cutoff,
            Post.status.in_(terminal),
            Post.id.not_in(pending_posts),
        )
        result = self._s.execute(stmt)
        return result.rowcount or 0
