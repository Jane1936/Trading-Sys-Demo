import sqlite3
from decimal import Decimal

from trailing_reduction_tracker import TrailingReductionTracker


class FakeAccountManager:
    def validate_config(self):
        return None


class FakeTradingExperiment:
    def __init__(self, db_path, account_manager=None, config=None):
        self.db_path = db_path

    def _fetch_experiment_usdt_equity(self):
        return Decimal("1000")

    def _fetch_and_store_positions(self):
        return [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1",
                "entryPrice": "90",
                "markPrice": "94",
                "unRealizedProfit": "15",
            }
        ]


def _create_klines(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE klines_15m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                low REAL NOT NULL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        conn.executemany(
            "INSERT INTO klines_15m (symbol, open_time, low) VALUES (?, ?, ?)",
            [("BTC", 1000, 97), ("BTC", 2000, 95)],
        )


def test_trailing_reduction_marks_pretrigger_structure_break(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    _create_klines(db_path)
    monkeypatch.setattr("trailing_reduction_tracker.TradingExperiment", FakeTradingExperiment)

    tracker = TrailingReductionTracker(db_path=str(db_path), account_manager=FakeAccountManager())
    result = tracker.run_round(decision_round_ts=2000)

    assert result["checked"] == 1
    assert result["eligible"] == 1
    assert result["pretriggered"] == 1
    round_ts, checks = tracker.get_latest_round_checks()
    assert round_ts == 2000
    assert checks[0].symbol == "BTC"
    assert checks[0].lowest_15m_low == "95"
    assert checks[0].tag == "预触发结构破位"
    assert checks[0].pretriggered is True
    assert tracker.summary_payload()["checks"][0]["latest_pretrigger_round_ts"] == 2000


def test_summary_payload_includes_recent_7_day_records(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    _create_klines(db_path)
    monkeypatch.setattr("trailing_reduction_tracker.TradingExperiment", FakeTradingExperiment)

    now_ms = 1_700_000_000_000
    monkeypatch.setattr("trailing_reduction_tracker.time.time", lambda: now_ms / 1000)
    tracker = TrailingReductionTracker(db_path=str(db_path), account_manager=FakeAccountManager())
    tracker.run_round(decision_round_ts=2000)
    tracker._insert_record(
        "BTC", 1000, now_ms - 3 * 24 * 60 * 60 * 1000, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"),
        Decimal("1"), Decimal("0.3"), Decimal("0.7"), "recent_old_round", "", "", "submitted", "recent_old_round", ""
    )
    tracker._insert_record(
        "BTC", 2000, now_ms - 1_000, Decimal("2"), Decimal("2"), Decimal("2"), Decimal("1"), Decimal("2"),
        Decimal("1"), Decimal("0.3"), Decimal("0.7"), "current", "", "", "submitted", "current_round", ""
    )
    tracker._insert_record(
        "BTC", 3000, now_ms - 8 * 24 * 60 * 60 * 1000, Decimal("3"), Decimal("3"), Decimal("3"), Decimal("1"), Decimal("3"),
        Decimal("1"), Decimal("0.3"), Decimal("0.7"), "expired", "", "", "submitted", "expired", ""
    )

    payload = tracker.summary_payload()

    assert payload["round_ts"] == 2000
    assert [record["market_order_id"] for record in payload["records"]] == ["current", "recent_old_round"]
