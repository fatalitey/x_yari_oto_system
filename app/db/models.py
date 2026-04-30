from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PostStatus(str, enum.Enum):
    raw = "raw"
    dropped = "dropped"
    candidate = "candidate"
    awaiting_approval = "awaiting_approval"
    processing_approval = "processing_approval"
    approved = "approved"
    rejected = "rejected"
    queued = "queued"
    published = "published"
    failed = "failed"
    hold = "hold"


class QueueStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


class Post(Base):
    __tablename__ = "posts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_label: Mapped[str] = mapped_column(String(255), default="")
    telegram_chat_id: Mapped[str] = mapped_column(String(64), index=True)
    telegram_message_id: Mapped[str] = mapped_column(String(64), index=True)
    original_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    rewritten_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    reaction_count: Mapped[int] = mapped_column(Integer, default=0)
    fingerprint_hash: Mapped[str] = mapped_column(String(128), index=True, default="")
    status: Mapped[str] = mapped_column(String(32), index=True, default=PostStatus.raw.value)

    queue_entries: Mapped[list["PublishQueue"]] = relationship(back_populates="post")


class PublishQueue(Base):
    __tablename__ = "publish_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default=QueueStatus.pending.value)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    post: Mapped["Post"] = relationship(back_populates="queue_entries")


class SystemState(Base):
    __tablename__ = "system_state"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
