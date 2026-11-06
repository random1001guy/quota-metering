"""Service settings, read from environment variables."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.environ.get("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
    redis_url: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    reserve_timeout_s: int = int(os.environ.get("RESERVE_TIMEOUT_S", "30"))
    idempotency_window_s: int = int(os.environ.get("IDEMPOTENCY_WINDOW_S", "3600"))
    lease_fraction: float = float(os.environ.get("LEASE_FRACTION", "0.05"))
    config_cache_ttl_s: int = int(os.environ.get("CONFIG_CACHE_TTL_S", "60"))


settings = Settings()
