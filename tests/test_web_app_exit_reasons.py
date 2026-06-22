import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import web_app
from holding_position_scoring import HoldingPositionScoringSystem
from partial_take_profit import PartialTakeProfitStrategy


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
