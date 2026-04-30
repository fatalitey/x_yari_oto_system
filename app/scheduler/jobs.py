from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.bots.dispatcher import dispatch_candidates
from app.collectors.telegram_collector import collect_once
from app.core.config import Settings, get_settings
from app.core.leader import LeaderElection
from app.core.storage import LocalMediaStorage
from app.db.models import PostStatus, QueueStatus
from app.db.repositories import CleanupRepository, PostRepository, PublishQueueRepository
from app.db.session import SessionLocal
from app.publishers.x_publisher import publish_one

logger = logging.getLogger(__name__)


def _scheduler_tzinfo(settings: Settings):
    name = (settings.scheduler_timezone or "UTC").strip()
    try:
        return ZoneInfo(name)
    except Exception:  # noqa: BLE001
        return timezone.utc


def job_telegram_round() -> None:
    """Önce kanalları çek, ardından onay grubuna aday gönder (2 saatlik döngü)."""
    job_fetch_telegram()
    job_dispatch_approval()


def _renew_leader(settings: Settings) -> bool:
    if not settings.leader_election_enabled:
        return True
    with SessionLocal() as session:
        el = LeaderElection(session, settings.node_id, settings.leader_lease_seconds)
        ok = el.try_acquire_or_renew()
        session.commit()
        return ok


def job_fetch_telegram() -> None:
    settings = get_settings()
    if not _renew_leader(settings):
        logger.debug("Lider değilim; Telegram toplama atlandı.")
        return
    media = settings.resolve_media_root()

    async def run() -> None:
        with SessionLocal() as session:
            await collect_once(session, settings, media)

    asyncio.run(run())


def job_dispatch_approval() -> None:
    settings = get_settings()
    if not _renew_leader(settings):
        return

    async def run() -> None:
        chat_count = max(len(settings.monitor_chat_list), 1)
        dispatch_limit = settings.dispatch_max_per_channel * chat_count
        with SessionLocal() as session:
            n = await dispatch_candidates(session, settings, limit=dispatch_limit)
            logger.info("Onay için gönderilen aday: %s", n)

    asyncio.run(run())


def job_publish() -> None:
    settings = get_settings()
    if not _renew_leader(settings):
        return
    with SessionLocal() as session:
        qrepo = PublishQueueRepository(session)
        posts = PostRepository(session)
        item = qrepo.next_due()
        if not item:
            return
        post = posts.get(item.post_id)
        if not post:
            qrepo.mark(item.id, QueueStatus.failed.value, "post yok")
            session.commit()
            return
        qrepo.mark(item.id, QueueStatus.processing.value)
        session.commit()
        try:
            publish_one(settings, post)
            qrepo.mark(item.id, QueueStatus.done.value)
            posts.set_status(post.id, PostStatus.published.value)
            session.commit()
            logger.info("Yayın tamamlandı post_id=%s", post.id)
        except Exception as e:  # noqa: BLE001
            logger.exception("Yayın hatası")
            qrepo.mark(item.id, QueueStatus.failed.value, str(e)[:500])
            posts.set_status(post.id, PostStatus.failed.value)
            session.commit()


def job_cleanup() -> None:
    settings = get_settings()
    if not _renew_leader(settings):
        return
    retain = max(settings.cleanup_retention_hours, 1)
    with SessionLocal() as session:
        c = CleanupRepository(session)
        # Post verisini günlük pencereyle tut: 24h (veya CLEANUP_RETENTION_HOURS).
        n0 = c.purge_posts_fetched_before(retain)
        n3 = c.purge_old_low_reaction_posts(retain, settings.post_min_reactions)
        n1 = c.purge_terminal_posts(retain)
        n2 = c.purge_completed_queue(retain)
        session.commit()
        logger.info(
            "Temizlik: fetched_eski=%s dusuk_tepki_eski=%s terminal_post=%s kuyruk=%s",
            n0,
            n3,
            n1,
            n2,
        )


def job_media_cleanup() -> None:
    settings = get_settings()
    if not _renew_leader(settings):
        return
    media_root = settings.resolve_media_root()
    storage = LocalMediaStorage(media_root)
    removed = storage.prune_older_than_hours(settings.media_retention_hours)
    logger.info("Medya temizliği: silinen_dosya=%s (saklama_saat=%s)", removed, settings.media_retention_hours)


def job_leader_heartbeat() -> None:
    settings = get_settings()
    if not settings.leader_election_enabled:
        return
    with SessionLocal() as session:
        LeaderElection(session, settings.node_id, settings.leader_lease_seconds).try_acquire_or_renew()
        session.commit()


def register_jobs(scheduler: BackgroundScheduler, settings: Settings) -> None:
    tz = settings.scheduler_timezone or "UTC"

    if settings.leader_election_enabled:
        hb = max(settings.leader_lease_seconds // 3, 30)
        scheduler.add_job(
            job_leader_heartbeat,
            IntervalTrigger(seconds=hb),
            id="leader_heartbeat",
            replace_existing=True,
        )

    if settings.fetch_on_the_hour:
        # Duvara kilitli cron (0,2,4…) yerine: süreç açıldıktan sonra her 2 saatte bir tam tur.
        # İlk tur main.py içinde RUN_INITIAL_FETCH_DISPATCH ile zaten atılır.
        tzinfo = _scheduler_tzinfo(settings)
        first = datetime.now(tzinfo) + timedelta(hours=2)
        scheduler.add_job(
            job_telegram_round,
            IntervalTrigger(hours=2, timezone=tz),
            id="telegram_round",
            next_run_time=first,
            replace_existing=True,
            misfire_grace_time=3600,
            coalesce=True,
            max_instances=1,
        )
    else:
        scheduler.add_job(
            job_fetch_telegram,
            IntervalTrigger(minutes=settings.fetch_interval_minutes),
            id="fetch_telegram",
            replace_existing=True,
        )
        scheduler.add_job(
            job_dispatch_approval,
            IntervalTrigger(minutes=max(settings.fetch_interval_minutes // 4, 5)),
            id="dispatch_approval",
            replace_existing=True,
        )

    if settings.publish_enabled:
        scheduler.add_job(
            job_publish,
            IntervalTrigger(minutes=settings.publish_interval_minutes),
            id="publish_x",
            replace_existing=True,
        )
    else:
        logger.info("Publish job devre dışı (PUBLISH_ENABLED=false).")
    scheduler.add_job(
        job_cleanup,
        IntervalTrigger(minutes=15),
        id="cleanup_pool",
        replace_existing=True,
    )
    scheduler.add_job(
        job_media_cleanup,
        CronTrigger(hour=3, minute=10, timezone=tz),
        id="cleanup_media_daily",
        replace_existing=True,
    )
