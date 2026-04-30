from __future__ import annotations

import logging
from pathlib import Path

from openai import OpenAI

from app.core.config import Settings

logger = logging.getLogger(__name__)


def _load_prompt(settings: Settings, source_text: str) -> str:
    path = settings.rewrite_prompt_file
    if not path.is_absolute():
        path = Path.cwd() / path
    template = path.read_text(encoding="utf-8")
    return template.replace("{{TEXT}}", source_text.strip())


def rewrite_for_x(settings: Settings, source_text: str) -> str:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY tanımlı değil.")
    client = OpenAI(api_key=settings.openai_api_key)
    prompt = _load_prompt(settings, source_text)
    resp = client.chat.completions.create(
        model=settings.openai_model,
        messages=[
            {"role": "system", "content": "You rewrite social posts for accuracy and clarity."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.4,
    )
    out = (resp.choices[0].message.content or "").strip()
    logger.info("OpenAI yeniden yazım tamamlandı (%s karakter)", len(out))
    return out
