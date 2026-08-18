import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app


def test_ensure_universe_retains_cached_snapshot_when_refresh_fails(monkeypatch):
    monkeypatch.setattr(app.collector, "UNIVERSE", ["BTC", "ETH"])
    monkeypatch.setattr(app, "_universe_last_refresh_ts", 0.0)
    monkeypatch.setattr(app.time, "time", lambda: 50_000.0)

    def fail_refresh():
        raise RuntimeError("temporary exchange outage")

    monkeypatch.setattr(app.collector, "build_universe", fail_refresh)

    assert app.ensure_universe() == ["BTC", "ETH"]
    assert app._universe_last_refresh_ts == (
        50_000.0
        - app._universe_refresh_interval_sec
        + app._universe_refresh_failure_retry_sec
    )


def test_ensure_universe_raises_refresh_error_without_cached_snapshot(monkeypatch):
    monkeypatch.setattr(app.collector, "UNIVERSE", None)
    monkeypatch.setattr(app, "_universe_last_refresh_ts", 0.0)

    def fail_refresh():
        raise RuntimeError("temporary exchange outage")

    monkeypatch.setattr(app.collector, "build_universe", fail_refresh)

    with pytest.raises(RuntimeError, match="temporary exchange outage"):
        app.ensure_universe()
