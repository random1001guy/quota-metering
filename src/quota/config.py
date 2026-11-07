"""Service settings, read from environment variables when Settings() is created."""
import os
from dataclasses import dataclass, field


def _env(name: str, default: str):
    return field(default_factory=lambda: os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    database_url: str = _env("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
    redis_url: str = _env("REDIS_URL", "redis://localhost:6379/0")
    pg_pool_size: int = field(default_factory=lambda: int(os.environ.get("PG_POOL_SIZE", "10")))
    redis_pool_size: int = field(default_factory=lambda: int(os.environ.get("REDIS_POOL_SIZE", "64")))
    # How long a reservation may stay open before its units are reclaimed. Must be longer than
    # the downstream operation's own timeout.
    reserve_timeout_s: int = field(default_factory=lambda: int(os.environ.get("RESERVE_TIMEOUT_S", "30")))
    # How long a finished reservation is remembered, so retries are answered, not re-charged.
    idempotency_window_s: int = field(default_factory=lambda: int(os.environ.get("IDEMPOTENCY_WINDOW_S", "3600")))
    # Lease chunk = this fraction of the monthly limit. Bounds what a Redis loss can write off.
    lease_fraction: float = field(default_factory=lambda: float(os.environ.get("LEASE_FRACTION", "0.05")))
    # Start a background refill when the balance drops below this fraction of a chunk.
    refill_watermark: float = field(default_factory=lambda: float(os.environ.get("REFILL_WATERMARK", "0.2")))
    refill_lock_ms: int = field(default_factory=lambda: int(os.environ.get("REFILL_LOCK_MS", "2000")))
    config_cache_ttl_s: int = field(default_factory=lambda: int(os.environ.get("CONFIG_CACHE_TTL_S", "60")))
