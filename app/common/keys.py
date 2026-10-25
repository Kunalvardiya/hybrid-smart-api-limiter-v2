"""Single place that defines every Redis key name used by the system."""

TELEMETRY_STREAM = "telemetry:events"
CONSUMER_GROUP = "behaviour-workers"
RISK_LOG_STREAM = "risk:log"       # append-only history of score records (capped)


def bucket_key(client_id: str) -> str:
    return f"bucket:{client_id}"


def policy_key(client_id: str) -> str:
    return f"policy:{client_id}"
