"""Central configuration, read from environment variables.

No secrets live in code: the Redis password comes from the environment
(.env locally, docker-compose in containers).
"""
import hashlib
import os
from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class Settings:
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    backend_url: str = "http://localhost:9000"
    bucket_capacity: int = 20
    bucket_refill_per_sec: float = 10.0
    # What the gateway does if Redis is unreachable: "open" lets traffic through
    # (availability first), "closed" rejects it with 503 (security first).
    fail_mode: str = "open"
    redis_timeout_s: float = 0.5
    redis_max_connections: int = 50    # gateway Redis pool size; extra requests wait (blocking pool)
    # Shared secret for the read-only /admin API and dashboard data. Empty = admin API disabled.
    admin_token: str = ""
    policy_refresh_s: float = 1.0      # how often the gateway copies the policy table
    policy_max_age_s: float = 120.0    # policy older than this is ignored (worker presumed down)
    blocked_retry_after_s: int = 60    # Retry-After sent to BLOCKED clients
    telemetry_tick_s: float = 1.0      # heartbeat event interval for the worker's clock; 0 disables
    telemetry_maxlen: int = 100_000
    telemetry_queue_size: int = 10_000
    # SHA-256 hashes of the API keys the gateway recognises. Empty = any key is
    # accepted as a client label (development only; see docs/TELEMETRY.md).
    allowed_key_hashes: frozenset = frozenset()

    @property
    def redis_url(self) -> str:
        auth = f":{quote(self.redis_password, safe='')}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/0"


def load_settings() -> Settings:
    env = os.environ
    fail_mode = env.get("RATE_LIMIT_FAIL_MODE", "open").lower()
    if fail_mode not in ("open", "closed"):
        raise ValueError("RATE_LIMIT_FAIL_MODE must be 'open' or 'closed'")
    keys = [k.strip() for k in env.get("ALLOWED_API_KEYS", "").split(",") if k.strip()]
    return Settings(
        admin_token=env.get("ADMIN_TOKEN", ""),
        policy_refresh_s=float(env.get("POLICY_REFRESH_S", "1.0")),
        policy_max_age_s=float(env.get("POLICY_MAX_AGE_S", "120")),
        blocked_retry_after_s=int(env.get("POLICY_DWELL_BLOCKED_S", "60")),
        telemetry_tick_s=float(env.get("TELEMETRY_TICK_S", "1.0")),
        telemetry_maxlen=int(env.get("TELEMETRY_STREAM_MAXLEN", "100000")),
        telemetry_queue_size=int(env.get("TELEMETRY_QUEUE_SIZE", "10000")),
        allowed_key_hashes=frozenset(hashlib.sha256(k.encode()).hexdigest() for k in keys),
        fail_mode=fail_mode,
        redis_timeout_s=float(env.get("REDIS_TIMEOUT_S", "0.5")),
        redis_max_connections=int(env.get("REDIS_MAX_CONNECTIONS", "50")),
        redis_host=env.get("REDIS_HOST", "localhost"),
        redis_port=int(env.get("REDIS_PORT", "6379")),
        redis_password=env.get("REDIS_PASSWORD", ""),
        backend_url=env.get("BACKEND_URL", "http://localhost:9000"),
        bucket_capacity=int(env.get("BUCKET_CAPACITY", "20")),
        bucket_refill_per_sec=float(env.get("BUCKET_REFILL_PER_SEC", "10")),
    )
