from __future__ import annotations

from enum import Enum
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


class AppEnvironment(str, Enum):
    development = "development"
    production = "production"
    test = "test"


class Settings(BaseSettings):
    app_name: str = "EYE"
    app_version: str = "0.1.0"
    debug: bool = Field(default=False)
    environment: AppEnvironment = Field(default=AppEnvironment.development)
    api_v1_prefix: str = "/api/v1"
    sqlite_db_path: Path = Field(default=DATA_DIR / "eye.db")
    external_intelligence_enabled: bool = Field(default=False)

    # Market data / realtime
    market_data_provider: str = Field(default="yfinance")
    market_data_realtime_enabled: bool = Field(default=False)
    market_data_poll_seconds: int = Field(default=5, ge=1, le=60)
    massive_api_key: str | None = Field(default=None)
    fmp_api_key: str = Field(default="")
    eia_api_key: str | None = Field(default=None)
    telegram_bot_token: str | None = Field(default=None)
    telegram_chat_id: str | None = Field(default=None)
    telegram_alerts_enabled: bool = Field(default=False)

    model_config = SettingsConfigDict(
        env_prefix="EYE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @property
    def is_production(self) -> bool:
        return self.environment == AppEnvironment.production


@lru_cache
def get_settings() -> Settings:
    return Settings()
