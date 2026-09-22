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


def test_alpha_summary_is_hidden_until_manual_refresh():
    row = {"symbol": "ACTIVE", "activity_1d": 1.25}
    trends = [{"symbol": "ACTIVE", "consecutive_up_days": 2}]
    with (
        patch("web_app.alpha_observer.latest_snapshot", return_value=(1_789_399_183_493, [row])),
        patch("web_app.alpha_observer.daily_trends", return_value=trends),
        patch("web_app._alpha_oi_changes", return_value=[]),
        patch("web_app._alpha_funding_changes", return_value=[]),
    ):
        response = app.test_client().get("/market/alpha/data?module=summary")

    assert response.status_code == 200
    assert response.get_json()["data"] == {
        "total_tokens": 1,
        "active_tokens": 1,
        "two_day_up_tokens": 1,
        "oi_abnormal_tokens": 0,
        "funding_abnormal_tokens": 0,
    }

    template = (Path(__file__).resolve().parents[1] / "templates" / "alpha.html").read_text()
    assert 'data-alpha-summary-value="total_tokens">—<' in template
    assert 'data-alpha-summary-value="active_tokens">—<' in template
    assert 'data-alpha-summary-value="two_day_up_tokens">—<' in template
    assert 'data-alpha-summary-value="oi_abnormal_tokens">—<' in template
    assert 'data-alpha-summary-value="funding_abnormal_tokens">—<' in template
    assert "data-alpha-summary-refresh" in template
    assert "module=summary" in template


def test_alpha_summary_counts_only_activity_at_least_one():
    rows = [
        {"symbol": "HIGH", "activity_1d": 1},
        {"symbol": "LOW", "activity_1d": 0.999},
        {"symbol": "MISSING", "activity_1d": None},
    ]
    trends = [
        {"symbol": "THREE_DAYS", "consecutive_up_days": 3},
        {"symbol": "TWO_DAYS", "consecutive_up_days": 2},
        {"symbol": "ONE_DAY", "consecutive_up_days": 1},
    ]
    with (
        patch("web_app.alpha_observer.latest_snapshot", return_value=(1_789_399_183_493, rows)),
        patch("web_app.alpha_observer.daily_trends", return_value=trends),
        patch("web_app._alpha_oi_changes", return_value=[]),
        patch("web_app._alpha_funding_changes", return_value=[]),
    ):
        response = app.test_client().get("/market/alpha/data?module=summary")

    assert response.status_code == 200
    assert response.get_json()["data"] == {
        "total_tokens": 3,
        "active_tokens": 1,
        "two_day_up_tokens": 2,
        "oi_abnormal_tokens": 0,
        "funding_abnormal_tokens": 0,
    }


def test_alpha_summary_counts_oi_and_funding_abnormal_symbols_at_boundaries():
    oi_changes = [
        {"symbol": "MATCH", "oi_change": 0.10, "price_change": -0.03},
        {"symbol": "LOW_OI", "oi_change": 0.0999, "price_change": 0.01},
        {"symbol": "HIGH_PRICE", "oi_change": 0.20, "price_change": 0.0301},
        {"symbol": "MISSING", "oi_change": None, "price_change": None},
    ]
    funding_changes = [
        {"symbol": "MATCH", "funding_change": 4.0, "price_change": 0.025},
        {"symbol": "LOW_FUNDING", "funding_change": 3.999, "price_change": 0.01},
        {"symbol": "HIGH_PRICE", "funding_change": 5.0, "price_change": -0.0251},
        {"symbol": "MISSING", "funding_change": None, "price_change": None},
    ]
    with (
        patch("web_app.alpha_observer.latest_snapshot", return_value=(1_789_399_183_493, [])),
        patch("web_app.alpha_observer.daily_trends", return_value=[]),
        patch("web_app._alpha_oi_changes", return_value=oi_changes),
        patch("web_app._alpha_funding_changes", return_value=funding_changes),
    ):
        response = app.test_client().get("/market/alpha/data?module=summary")

    assert response.status_code == 200
    assert response.get_json()["data"]["oi_abnormal_tokens"] == 1
    assert response.get_json()["data"]["funding_abnormal_tokens"] == 1


def test_alpha_deferred_modules_prompt_user_to_load_all_data():
    template = (Path(__file__).resolve().parents[1] / "templates" / "alpha.html").read_text()

    assert template.count("请点击加载全部数据按钮") == 3


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


def test_alpha_funding_changes_excludes_stale_symbols(tmp_path, monkeypatch):
    db_path = tmp_path / "alpha.db"
    now = int(time.time() * 1000)
    hour = 3_600_000
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE alpha_hourly_market "
            "(symbol TEXT, open_time INTEGER, close REAL, open_interest REAL, "
            "funding_rate REAL, PRIMARY KEY(symbol, open_time))"
        )
        conn.executemany(
            "INSERT INTO alpha_hourly_market VALUES (?, ?, ?, ?, ?)",
            [
                ("CURRENT", now - 5 * hour, 10, 100, 0.01),
                ("CURRENT", now - hour, 11, 110, 0.02),
                ("STALE", now - 30 * hour, 20, 200, 0.01),
                ("STALE", now - 25 * hour, 22, 220, 0.02),
            ],
        )
    monkeypatch.setattr(web_app, "ALPHA_DB_PATH", str(db_path))
    monkeypatch.setattr(
        web_app.alpha_observer,
        "latest_snapshot",
        lambda _path: (now, [{"symbol": "CURRENT"}, {"symbol": "STALE"}]),
    )
    web_app._alpha_analytics_cache.clear()

    assert web_app._alpha_funding_changes() == [
        {"symbol": "CURRENT", "funding_change": 1.0, "price_change": pytest.approx(0.1)}
    ]
