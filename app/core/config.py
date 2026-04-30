from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_prompt_path() -> Path:
    return Path(__file__).resolve().parent.parent / "ai" / "prompts" / "rewrite_default.txt"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    node_id: str = Field(default="default-node", validation_alias="NODE_ID")
    # Yerel tek cihazda false: DB bu makinede, lider seçimi yok. Ortak DB kullanan çok süreç için true.
    leader_election_enabled: bool = Field(default=False, validation_alias="LEADER_ELECTION_ENABLED")
    leader_lease_seconds: int = Field(default=120, ge=30, validation_alias="LEADER_LEASE_SECONDS")
    standalone_approval_bot: bool = Field(default=False, validation_alias="STANDALONE_APPROVAL_BOT")
    approval_bot_enabled: bool = Field(default=True, validation_alias="APPROVAL_BOT_ENABLED")

    # Varsayılan: proje altında yerel SQLite. Cihaz değişince bu dosya taşınmaz = sıfırdan başlarsınız.
    database_url: str = Field(
        default="sqlite:///./data/app.db",
        validation_alias="DATABASE_URL",
    )
    media_storage_root: Path = Field(
        default=Path("storage/media"),
        validation_alias="MEDIA_STORAGE_ROOT",
    )
    media_retention_hours: float = Field(default=24.0, ge=1.0, validation_alias="MEDIA_RETENTION_HOURS")
    draft_export_root: Path = Field(
        default=Path("storage/x_drafts"),
        validation_alias="DRAFT_EXPORT_ROOT",
    )

    telegram_api_id: int | None = Field(default=None, validation_alias="TELEGRAM_API_ID")
    telegram_api_hash: str | None = Field(default=None, validation_alias="TELEGRAM_API_HASH")
    telegram_session_string: str | None = Field(default=None, validation_alias="TELEGRAM_SESSION_STRING")
    telegram_monitor_chats: str = Field(default="", validation_alias="TELEGRAM_MONITOR_CHATS")
    telegram_fetch_limit: int = Field(default=100, ge=10, le=500, validation_alias="TELEGRAM_FETCH_LIMIT")
    telegram_bot_token: str | None = Field(default=None, validation_alias="TELEGRAM_BOT_TOKEN")
    # Kişisel sohbet id'si VEYA süper grup id'si (-100...). İkisi birden: TELEGRAM_APPROVAL_CHAT_ID öncelikli.
    telegram_approval_chat_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("TELEGRAM_APPROVAL_CHAT_ID", "TELEGRAM_ADMIN_USER_ID"),
    )

    openai_api_key: str | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", validation_alias="OPENAI_MODEL")
    rewrite_prompt_file: Path = Field(default_factory=_default_prompt_path, validation_alias="REWRITE_PROMPT_FILE")

    x_api_key: str | None = Field(default=None, validation_alias="X_API_KEY")
    x_api_secret: str | None = Field(default=None, validation_alias="X_API_SECRET")
    x_access_token: str | None = Field(default=None, validation_alias="X_ACCESS_TOKEN")
    x_access_token_secret: str | None = Field(default=None, validation_alias="X_ACCESS_TOKEN_SECRET")
    x_bearer_token: str | None = Field(default=None, validation_alias="X_BEARER_TOKEN")

    fetch_interval_minutes: int = Field(default=60, ge=1, validation_alias="FETCH_INTERVAL_MINUTES")
    # true: Telegram çekimi 2 saatte bir :00'da (SCHEDULER_TIMEZONE ile). false: interval ile FETCH_INTERVAL_MINUTES.
    fetch_on_the_hour: bool = Field(default=True, validation_alias="FETCH_ON_THE_HOUR")
    # Uygulama açılır açılmaz bir fetch + dispatch turu koşturur.
    run_initial_fetch_dispatch: bool = Field(default=True, validation_alias="RUN_INITIAL_FETCH_DISPATCH")
    scheduler_timezone: str = Field(default="Europe/Istanbul", validation_alias="SCHEDULER_TIMEZONE")
    # Her dispatch turunda bir kaynak kanaldan en fazla kaç aday gönderilsin.
    dispatch_max_per_channel: int = Field(default=3, ge=1, le=20, validation_alias="DISPATCH_MAX_PER_CHANNEL")
    publish_enabled: bool = Field(default=True, validation_alias="PUBLISH_ENABLED")
    publish_interval_minutes: int = Field(default=30, ge=1, validation_alias="PUBLISH_INTERVAL_MINUTES")
    cleanup_retention_hours: int = Field(default=24, ge=1, validation_alias="CLEANUP_RETENTION_HOURS")
    # Havuz: bu süreden eski fetched_at ile kayıt tutulmaz (silinir).
    pool_max_fetched_age_hours: float = Field(default=2.0, ge=0.25, validation_alias="POOL_MAX_FETCHED_AGE_HOURS")
    post_max_age_hours: float = Field(default=2.0, ge=0.1, validation_alias="POST_MAX_AGE_HOURS")
    # Yalnızca cleanup'ta düşük etkileşimli eski kayıtları kırpmak için kullanılır.
    post_min_reactions: int = Field(default=100, ge=0, validation_alias="POST_MIN_REACTIONS")
    near_duplicate_similarity: float = Field(default=0.92, ge=0.5, le=1.0, validation_alias="NEAR_DUPLICATE_SIMILARITY")

    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    @field_validator("telegram_api_id", "telegram_approval_chat_id", mode="before")
    @classmethod
    def int_or_empty(cls, v: object) -> object:
        if v is None or v == "":
            return None
        return int(v)

    @field_validator("telegram_fetch_limit", mode="before")
    @classmethod
    def fetch_limit_coerce(cls, v: object) -> object:
        if v is None or v == "":
            return 100
        return int(v)

    @property
    def monitor_chat_list(self) -> list[str]:
        parts = [p.strip() for p in self.telegram_monitor_chats.split(",") if p.strip()]
        return parts

    def resolve_media_root(self, project_root: Path | None = None) -> Path:
        root = project_root or Path.cwd()
        p = self.media_storage_root
        if not p.is_absolute():
            p = (root / p).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    def resolve_draft_export_root(self, project_root: Path | None = None) -> Path:
        root = project_root or Path.cwd()
        p = self.draft_export_root
        if not p.is_absolute():
            p = (root / p).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p


@lru_cache
def get_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
