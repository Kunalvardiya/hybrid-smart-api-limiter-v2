import pytest


@pytest.fixture(autouse=True)
def _no_background_ticks(monkeypatch):
    """The gateway emits a heartbeat 'tick' event every second in production. Most tests count stream
    entries exactly, so ticks are off unless a test turns them on (see TELEMETRY_TICK_S tests)."""
    monkeypatch.setenv("TELEMETRY_TICK_S", "0")
