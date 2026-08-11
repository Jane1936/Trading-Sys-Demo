import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import web_app
from holding_position_scoring import HoldingPositionScoringSystem
from partial_take_profit import PartialTakeProfitStrategy
from trailing_reduction_tracker import TrailingReductionTracker
from trailing_stop_tracker import TrailingStopTracker
from zombie_force_liquidation import ZombieForceLiquidationModule


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
            CREATE TABLE {HoldingPositionScoringSystem.REDUCTION_STOP_FAILURE_LIQUIDATIONS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity TEXT NOT NULL,
                liquidation_market_order_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
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
        conn.execute(
            f"""
            CREATE TABLE {TrailingReductionTracker.RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                decision_round_ts INTEGER NOT NULL,
                checked_at INTEGER NOT NULL,
                reduced_quantity TEXT NOT NULL,
                market_order_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE {HoldingPositionScoringSystem.INCREASE_RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                increased_quantity TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE {ZombieForceLiquidationModule.RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                checked_at INTEGER NOT NULL,
                opened_at INTEGER,
                side TEXT NOT NULL,
                position_amt TEXT NOT NULL,
                quantity TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                raw_response TEXT NOT NULL DEFAULT ''
            )
            """
        )


def test_filled_sell_order_exit_reason_uses_zombie_force_liquidation_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {ZombieForceLiquidationModule.RECORDS_TABLE} (symbol, checked_at, opened_at, side, position_amt, quantity, entry_price, status, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", 1000, 1, "SELL", "2", "2.00", "10", "submitted", "zombie_position_force_liquidation", "{}"),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "2", "realized_pnl": "-1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "僵尸强平"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "僵尸强平", "matched_at": "1000"}]
    assert "zombie_position_force_liquidation" not in str(annotated["orders"][0])



def test_filled_sell_order_exit_reason_uses_zombie_force_liquidation_order_id_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {ZombieForceLiquidationModule.RECORDS_TABLE} (symbol, checked_at, opened_at, side, position_amt, quantity, entry_price, status, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "BANK",
                1000,
                1,
                "SELL",
                "2",
                "2.00",
                "10",
                "submitted",
                "zombie_position_force_liquidation",
                "{'open_orders_cancel': {'code': 200}} | {'market_close': {'orderId': 98765, 'executedQty': '2'}}",
            ),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "order_id": "98765", "side": "SELL", "time": 1000, "quantity": "1", "realized_pnl": "1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "僵尸强平"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "僵尸强平", "matched_at": "1000"}]


def test_filled_sell_order_exit_reason_uses_zombie_force_liquidation_stored_order_id_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"ALTER TABLE {ZombieForceLiquidationModule.RECORDS_TABLE} ADD COLUMN order_id TEXT NOT NULL DEFAULT ''")
        conn.execute(
            f"INSERT INTO {ZombieForceLiquidationModule.RECORDS_TABLE} (symbol, checked_at, opened_at, side, position_amt, quantity, entry_price, status, order_id, reason, raw_response) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", 1000, 1, "SELL", "2", "2.00", "10", "submitted", "98765", "zombie_position_force_liquidation", "{}"),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "order_id": "98765", "side": "SELL", "time": 1000, "quantity": "1", "realized_pnl": "1"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "僵尸强平"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "僵尸强平", "matched_at": "1000"}]

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


def test_reduction_failure_liquidation_takes_priority_over_reduction_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.REDUCTION_RECORDS_TABLE} "
            "(symbol, side, reduced_quantity, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            ("BANK", "SELL", "1", "normal reduction", 1000),
        )
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.REDUCTION_STOP_FAILURE_LIQUIDATIONS_TABLE} "
            "(symbol, side, quantity, liquidation_market_order_id, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("BANK", "SELL", "1", "force-close-123", "submitted", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    payload = {"orders": [{
        "symbol": "BANKUSDT", "order_id": "force-close-123", "side": "SELL",
        "time": 1000, "quantity": "1", "realized_pnl": "-2",
    }]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "减仓失败强平"
    assert {match["type"] for match in annotated["orders"][0]["exit_reason_matches"]} == {"减仓失败强平", "减仓"}


def test_short_reduction_failure_liquidation_matches_buy_fill(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.REDUCTION_STOP_FAILURE_LIQUIDATIONS_TABLE} "
            "(symbol, side, quantity, liquidation_market_order_id, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("BANK", "BUY", "2", "short-close-456", "submitted", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    payload = {"orders": [{
        "symbol": "BANKUSDT", "order_id": "short-close-456", "side": "BUY",
        "time": 1000, "quantity": "2", "realized_pnl": "-3",
    }]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "减仓失败强平"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "减仓失败强平", "matched_at": "1000"}]


def test_filled_sell_order_exit_reason_uses_trailing_reduction_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {TrailingReductionTracker.RECORDS_TABLE} (symbol, decision_round_ts, checked_at, reduced_quantity, market_order_id, status) VALUES (?, ?, ?, ?, ?, ?)",
            ("BANK", 900, 1000, "2.5", "888001", "submitted"),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "SELL", "time": 1000, "quantity": "2.500", "order_id": "888001", "realized_pnl": "3"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "移动追踪减仓"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "移动追踪减仓", "matched_at": "1000"}]


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


def test_filled_buy_order_exit_reason_uses_increase_match(tmp_path, monkeypatch):
    db_path = tmp_path / "orders.db"
    _create_exit_reason_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {HoldingPositionScoringSystem.INCREASE_RECORDS_TABLE} (symbol, status, increased_quantity, created_at) VALUES (?, ?, ?, ?)",
            ("BANK", "submitted", "4.50", 1000),
        )
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))

    payload = {"orders": [{"symbol": "BANKUSDT", "side": "BUY", "time": 1000, "quantity": "4.5", "realized_pnl": "0"}]}

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "加仓"
    assert annotated["orders"][0]["exit_reason_matches"] == [{"type": "加仓", "matched_at": "1000"}]

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


def test_filled_order_annotation_adds_leverage_and_rule_scores(tmp_path, monkeypatch):
    db_path = tmp_path / "scores.db"
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    rule_columns = ", ".join(f"rule{i}_score INTEGER NOT NULL" for i in range(1, 19))
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"CREATE TABLE {web_app.TradingExperiment.TRADES_TABLE} "
            "(id INTEGER PRIMARY KEY, symbol TEXT, status TEXT, total_score INTEGER, "
            "decision_round_ts INTEGER, leverage INTEGER, created_at INTEGER)"
        )
        conn.execute(
            f"CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, "
            f"{rule_columns}, total_score INTEGER)"
        )
        conn.execute(
            f"INSERT INTO {web_app.TradingExperiment.TRADES_TABLE} VALUES "
            "(1, 'BANK', 'opened', 82, 900, 8, 950)"
        )
        scores = tuple(range(1, 19))
        placeholders = ", ".join("?" for _ in range(21))
        conn.execute(
            f"INSERT INTO symbol_total_scores VALUES ({placeholders})",
            ("BANK", 900, *scores, sum(scores)),
        )

    annotated = web_app._annotate_filled_order_exit_reasons(
        {"orders": [{"symbol": "BANKUSDT", "side": "BUY", "time": 1000, "quantity": "1"}]}
    )["orders"][0]

    assert annotated["open_leverage"] == 8
    assert [annotated[f"open_rule{i}_score"] for i in range(1, 19)] == list(range(1, 19))


def test_filled_order_annotation_reads_migrated_records_from_trading_core_db(
    tmp_path, monkeypatch
):
    trading_db = tmp_path / "trading.db"
    core_db = tmp_path / "trading_core.db"
    monkeypatch.setattr(web_app, "DB_PATH", web_app.BASE_DB_PATH)
    monkeypatch.setattr(web_app, "TRADING_DB_PATH", str(trading_db))
    monkeypatch.setattr(web_app.db_config, "TRADING_DB_PATH", str(trading_db))
    monkeypatch.setattr(web_app.db_config, "TRADING_CORE_DB_PATH", str(core_db))

    with sqlite3.connect(trading_db) as conn:
        conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    with sqlite3.connect(core_db) as conn:
        conn.execute(
            f"""
            CREATE TABLE {ZombieForceLiquidationModule.RECORDS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                checked_at INTEGER NOT NULL,
                side TEXT NOT NULL,
                quantity TEXT NOT NULL,
                status TEXT NOT NULL,
                order_id TEXT NOT NULL DEFAULT '',
                raw_response TEXT NOT NULL DEFAULT ''
            )
            """
        )
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
            f"INSERT INTO {ZombieForceLiquidationModule.RECORDS_TABLE} "
            "(symbol, checked_at, side, quantity, status, order_id, raw_response) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("BANK", 1000, "SELL", "2", "submitted", "9001", "{}"),
        )
        conn.execute(
            f"INSERT INTO {web_app.TradingExperiment.TRADES_TABLE} "
            "(symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
            ("BANK", "opened", 90, 900),
        )

    payload = {
        "orders": [
            {
                "symbol": "BANKUSDT",
                "order_id": "9001",
                "side": "SELL",
                "time": 1000,
                "quantity": "2",
                "realized_pnl": "-1",
            }
        ]
    }

    annotated = web_app._annotate_filled_order_exit_reasons(payload)

    assert annotated["orders"][0]["exit_reason"] == "僵尸强平"
    assert annotated["orders"][0]["open_total_score"] == 90
