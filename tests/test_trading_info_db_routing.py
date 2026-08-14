import sqlite3
from decimal import Decimal

import db_config
from break_even_take_profit import BreakEvenTakeProfitStrategy
from holding_position_scoring import HoldingPositionScoringSystem
from partial_take_profit import PartialTakeProfitStrategy
from trailing_reduction_tracker import TrailingReductionTracker


INFO_TABLES = {
    "break_even_stop_loss_records",
    "holding_position_increase_records",
    "partial_take_profit_error_records",
    "partial_take_profit_records",
    "trailing_reduction_records",
}


def _table_names(path):
    with sqlite3.connect(path) as conn:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def test_production_action_record_tables_route_to_trading_info_db(tmp_path, monkeypatch):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    info_db = str(tmp_path / "trading_info.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", info_db)
    monkeypatch.setattr(db_config, "BASE_DB_PATH", str(tmp_path / "base.db"))
    monkeypatch.setattr(db_config, "SCORING_DB_PATH", str(tmp_path / "scoring.db"))
    monkeypatch.setattr(db_config, "MARKET_DB_PATH", str(tmp_path / "market.db"))

    BreakEvenTakeProfitStrategy(db_path=trading_db).init_tables()
    PartialTakeProfitStrategy(db_path=trading_db).init_tables()
    TrailingReductionTracker(db_path=trading_db).init_tables()
    HoldingPositionScoringSystem(db_path=trading_db).init_tables()

    assert INFO_TABLES <= _table_names(info_db)
    assert INFO_TABLES.isdisjoint(_table_names(trading_db))


def test_cross_module_partial_take_profit_reads_use_trading_info_db(
    tmp_path, monkeypatch
):
    trading_db = str(tmp_path / "trading.db")
    info_db = str(tmp_path / "trading_info.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", info_db)

    partial = PartialTakeProfitStrategy(db_path=trading_db)
    partial.init_tables()
    with db_config.connect_sqlite(info_db) as conn:
        conn.execute(
            f"""
            INSERT INTO {partial.RECORDS_TABLE}
            (symbol, checked_at, side, position_amt, take_profit_quantity,
             entry_price, account_equity_usdt, r_usdt, trigger_r_usdt,
             unrealized_pnl, status, reason)
            VALUES ('BTC', 2000, 'SELL', '1', '0.3', '100', '1000', '10',
                    '20', '25', 'submitted', 'ok')
            """
        )

    holding = HoldingPositionScoringSystem(db_path=trading_db)
    trailing = TrailingReductionTracker(db_path=trading_db)
    assert partial._has_success_record("BTC", Decimal("100"))
    assert holding._has_partial_take_profit_record_since("BTC", 1500)
    assert trailing._has_partial_take_profit_record_since("BTC", 1500)


def test_custom_database_keeps_single_file_compatibility(tmp_path):
    custom_db = str(tmp_path / "custom.db")
    assert db_config.trading_info_path(custom_db) == custom_db

    BreakEvenTakeProfitStrategy(db_path=custom_db).init_tables()
    assert "break_even_stop_loss_records" in _table_names(custom_db)
