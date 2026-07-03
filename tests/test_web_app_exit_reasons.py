import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import web_app
from holding_position_scoring import HoldingPositionScoringSystem
from partial_take_profit import PartialTakeProfitStrategy
from trailing_stop_tracker import TrailingStopTracker


def _create_exit_reason_tables(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE {HoldingPositionScoringSystem.RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE {PartialTakeProfitStrategy.RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                take_profit_quantity TEXT NOT NULL,
                reason TEXT NOT NULL,
                checked_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE {HoldingPositionScoringSystem.REDUCTION_RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                reduced_quantity TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE {TrailingStopTracker.CHECKS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                checked_at INTEGER NOT NULL,
                close_quantity TEXT NOT NULL,
                trailing_stop_triggered INTEGER NOT NULL,
                close_status TEXT NOT NULL
            )
            """
        )


def test_filled_sell_order_exit_reason_uses_structural_stop_loss_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.RECORDS_TABLE} (symbol, side, quantity, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            ("BANK", "SELL", "2.0", "two_15m_closes_below_structural_stop_loss", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "2", "realized_pnl": "-1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "结构止损"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "结构止损", "matched_at": "1000"}]
    assert "two_15m_closes_below_structural_stop_loss" not in str(annotated["orders"][0])


def test_filled_sell_order_exit_reason_uses_partial_take_profit_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE} (symbol, side, take_profit_quantity, reason, checked_at) VALUES (?, ?, ?, ?, ?)",
            ("BANK", "SELL", "0.30", "unrealized_pnl_ge_2r_take_profit_30_percent", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "0.3", "realized_pnl": "2"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "分批止盈"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "分批止盈", "matched_at": "1000"}]
    assert "unrealized_pnl_ge_2r_take_profit_30_percent" not in str(annotated["orders"][0])


def test_filled_sell_order_exit_reason_uses_reduction_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.REDUCTION_RECORDS_TABLE} (symbol, side, reduced_quantity, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            ("BANK", "SELL", "1.25", "matched_rule=rule4; reduction_percent=50%", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "1.250", "realized_pnl": "1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "减仓"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "减仓", "matched_at": "1000"}]
    assert "reduction_percent" not in str(annotated["orders"][0])


def test_filled_sell_order_exit_reason_uses_trailing_stop_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {TrailingStopTracker.CHECKS_TABLE} (symbol, checked_at, close_quantity, trailing_stop_triggered, close_status) VALUES (?, ?, ?, ?, ?)",
            ("BANK", 1000, "7", 1, "submitted"),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "7.0", "realized_pnl": "3"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "移动追踪止盈"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "移动追踪止盈", "matched_at": "1000"}]


def test_unmatched_filled_sell_order_exit_reason_falls_back_to_realized_pnl(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {
        "orders": [
            {"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "1", "realized_pnl": "0.01"},
            {"symbol": "TREEUSDT", "side": "SELL", "time": 1000, "quantity": "1", "realized_pnl": "0"},
            {"symbol": "BANKUSDT", "side": "BUY", "time": 1000, "quantity": "1", "realized_pnl": "5"},
        ]
    }

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert [order["exit_reason"] for order in annotated["orders"]] == ["硬止盈", "硬止损", ""]


def test_trailing_stop_summary_api_returns_latest_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    TrailingStopTracker(db_path=str(db_path)).init_tables()

    client = web_app.app.test_client()
    response = client.get("/api/trailing-stop/summary")

    assert response.status_code == 200
    assert response.get_json() == {"round_ts": None, "checks": [], "records": []}


def test_filled_order_annotation_adds_open_score_from_latest_trade_record(tmp_path, monkeypatch):
    db_path = tmp_path / "scores.db"
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            CREATE TABLE {web_app.TradingExperiment.TRADES_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                total_score INTEGER,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"INSERT INTO {web_app.TradingExperiment.TRADES_TABLE} (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
            ("BANK", "opened", 82, 800),
        )
        conn.execute(
            f"INSERT INTO {web_app.TradingExperiment.TRADES_TABLE} (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
            ("BANK", "opened", 90, 1100),
        )

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "BUY", "time": 1000, "quantity": "1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["open_total_score"] == 90
    assert annotated["orders"][0]["open_score_band"] == "确定性强趋势单"
    assert annotated["orders"][0]["open_score_matched_at"] == "1100"
