from __future__ import annotations

import logging
from pathlib import Path

import tweepy

from app.db.models import Post

logger = logging.getLogger(__name__)


def _client_from_settings(settings) -> tweepy.Client:
    return tweepy.Client(
        consumer_key=settings.x_api_key,
        consumer_secret=settings.x_api_secret,
        access_token=settings.x_access_token,
        access_token_secret=settings.x_access_token_secret,
    )


def publish_one(settings, post: Post) -> str:
    """X'e tek gönderi döndürür: yayınlanan tweet id."""
    text = (post.rewritten_text or post.original_text or "").strip()
    if not text:
        raise ValueError("Yayınlanacak metin yok.")

    client = _client_from_settings(settings)
    media_ids = []
    if post.media_path and Path(post.media_path).is_file():
        # Medya için v1.1 uploader gerekir
        auth = tweepy.OAuth1UserHandler(
            settings.x_api_key,
            settings.x_api_secret,
            settings.x_access_token,
            settings.x_access_token_secret,
        )
        api = tweepy.API(auth)
        media = api.media_upload(post.media_path)
        media_ids.append(media.media_id_string)

    if media_ids:
        resp = client.create_tweet(text=text, media_ids=media_ids)
    else:
        resp = client.create_tweet(text=text)
    tid = ""
    if resp and resp.data:
        data = resp.data
        tid = str(data.get("id", getattr(data, "id", "")))
    logger.info("X yayını oluşturuldu: %s", tid)
    return str(tid)
