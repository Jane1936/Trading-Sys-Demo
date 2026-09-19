from pathlib import Path
import sqlite3
import sys
import time
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import alpha_observer
import web_app
from web_app import app


def _create_market_tables(path):
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE open_interest_1m "
            "(symbol TEXT, snapshot_time INTEGER, open_interest REAL, "
            "PRIMARY KEY(symbol, snapshot_time))"
        )
        for interval in ("1h", "4h"):
            conn.execute(
                f"CREATE TABLE klines_{interval} "
                "(symbol TEXT, open_time INTEGER, close REAL, funding_rate REAL, "
                "PRIMARY KEY(symbol, open_time))"
            )


def test_empty_alpha_page_reports_database_actually_read():
    status = alpha_observer.AlphaDatabaseStatus(
        path="/app/custom/alpha.db",
        size_bytes=4096,
        total_rows=0,
        snapshot_count=0,
    )
    with (
        patch("web_app.alpha_observer.latest_snapshot", return_value=(None, [])),
        patch("web_app.alpha_observer.database_status", return_value=status),
    ):
        response = app.test_client().get("/market/alpha")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "/app/custom/alpha.db" in body
    assert "表内共有" in body
    assert "ALPHA_DB_PATH" in body


def test_populated_alpha_page_marks_standalone_panel_visible():
    row = {
        "symbol": "ALPHA",
        "name": "Alpha",
        "chain_id": "56",
        "contract_address": "0x1",
        "icon_url": "",
        "volume_24h": "123.4",
        "market_cap": "567.8",
        "observed_at": 1_789_399_183_493,
    }
    status = alpha_observer.AlphaDatabaseStatus(
        path="/app/data/alpha.db",
        size_bytes=4096,
        total_rows=1,
        snapshot_count=1,
    )
    with (
        patch(
            "web_app.alpha_observer.latest_snapshot",
            return_value=(row["observed_at"], [row]),
        ),
        patch("web_app.alpha_observer.database_status", return_value=status),
    ):
        response = app.test_client().get("/market/alpha")

    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert '<section class="panel alpha-panel active">' in body
    assert "ALPHA" in body
    assert "2天活跃度" in body
    assert "5天活跃度" in body


def test_alpha_trend_table_omits_trend_return_metric():
    template = (Path(__file__).resolve().parents[1] / "templates" / "alpha.html").read_text()

    assert "趋势收益" not in template
    assert "trend_return" not in template


def test_alpha_page_refreshes_snapshot_at_hourly_minute_one_without_meta_refresh():
    template = (Path(__file__).resolve().parents[1] / "templates" / "alpha.html").read_text()

    assert 'http-equiv="refresh"' not in template
    assert "nextRefresh.setUTCMinutes(1, 0, 0)" in template
    assert "loadAlphaModule('snapshot')" in template


def test_alpha_oi_changes_batches_symbols_and_caches_by_snapshot(tmp_path, monkeypatch):
    db_path = tmp_path / "market.db"
    _create_market_tables(db_path)
    now = int(time.time() * 1000)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO open_interest_1m VALUES (?, ?, ?)",
            [
                ("AAA", now - 3_600_000, 100), ("AAA", now, 125),
                ("BBB", now - 3_600_000, 200), ("BBB", now, 180),
            ],
        )
        conn.executemany(
            "INSERT INTO klines_1h VALUES (?, ?, ?, NULL)",
            [("AAA", 1, 10), ("AAA", 2, 11), ("BBB", 1, 20), ("BBB", 2, 18)],
        )
    snapshots = [(10, [{"symbol": "AAA"}, {"symbol": "BBBUSDT"}])]
    monkeypatch.setattr(web_app, "BASE_DB_PATH", str(db_path))
    monkeypatch.setattr(web_app.alpha_observer, "latest_snapshot", lambda _path: snapshots[0])
    web_app._alpha_analytics_cache.clear()

    first = web_app._alpha_oi_changes()
    assert [(row["symbol"], row["oi_change"], row["price_change"]) for row in first] == [
        ("AAA", 0.25, pytest.approx(0.1)),
        ("BBB", pytest.approx(-0.1), pytest.approx(-0.1)),
    ]

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE open_interest_1m SET open_interest=150 "
            "WHERE symbol='AAA' AND snapshot_time=?", (now,)
        )
    assert web_app._alpha_oi_changes() == first
    snapshots[0] = (11, snapshots[0][1])
    assert web_app._alpha_oi_changes()[0]["oi_change"] == 0.5


def test_alpha_funding_changes_batches_symbols(tmp_path, monkeypatch):
    db_path = tmp_path / "market.db"
    _create_market_tables(db_path)
    hour = 3_600_000
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO klines_1h VALUES (?, ?, ?, ?)",
            [
                # Adjacent hourly rows intentionally have equal rates.  Funding
                # rates commonly remain unchanged for several hours, so the
                # comparison must use the four-hour anchor rather than rn=2.
                ("AAA", 0, 10, 0.01),
                ("AAA", 3 * hour, 10.5, 0.02),
                ("AAA", 4 * hour, 11, 0.02),
                ("BBB", 0, 20, 0.02),
                ("BBB", 3 * hour, 19, 0.01),
                ("BBB", 4 * hour, 18, 0.01),
            ],
        )
        conn.executemany(
            "INSERT INTO klines_4h VALUES (?, ?, ?, NULL)",
            [("AAA", 1, 10), ("AAA", 2, 12), ("BBB", 1, 20), ("BBB", 2, 18)],
        )
    monkeypatch.setattr(web_app, "BASE_DB_PATH", str(db_path))
    monkeypatch.setattr(
        web_app.alpha_observer,
        "latest_snapshot",
        lambda _path: (20, [{"symbol": "AAA"}, {"symbol": "BBB"}]),
    )
    web_app._alpha_analytics_cache.clear()

    rows = web_app._alpha_funding_changes()

    assert [(row["symbol"], row["funding_change"], row["price_change"]) for row in rows] == [
        ("AAA", 1.0, pytest.approx(0.1)),
        ("BBB", -0.5, pytest.approx(-0.1)),
    ]


def test_alpha_funding_change_requires_four_hours_of_history(tmp_path, monkeypatch):
    db_path = tmp_path / "market.db"
    _create_market_tables(db_path)
    hour = 3_600_000
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO klines_1h VALUES (?, ?, ?, ?)",
            [("AAA", 2 * hour, 10, 0.01), ("AAA", 4 * hour, 11, 0.02)],
        )
    monkeypatch.setattr(web_app, "BASE_DB_PATH", str(db_path))
    monkeypatch.setattr(
        web_app.alpha_observer,
        "latest_snapshot",
        lambda _path: (21, [{"symbol": "AAA"}]),
    )
    web_app._alpha_analytics_cache.clear()

    assert web_app._alpha_funding_changes() == [
        {"symbol": "AAA", "funding_change": None, "price_change": None}
    ]
