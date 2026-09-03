import sqlite3
from decimal import Decimal

import db_config
from hard_take_profit import HardTakeProfit


class FakeHardTakeProfit(HardTakeProfit):
    def _latest_open_trade(self, symbol):
        return 7, 1_000

    def _execute_close(self, exchange_symbol, symbol, amount, now):
        self.closed = (exchange_symbol, symbol, amount)
        return abs(amount), "order-1", "submitted", "submitted"


def test_hard_take_profit_triggers_at_twenty_percent(tmp_path):
    module = FakeHardTakeProfit(str(tmp_path / "trading.db"))
    module.init_tables()

    assert module._evaluate_position({
        "symbol": "BTCUSDT", "positionAmt": "2", "entryPrice": "100",
        "unRealizedProfit": "40",
    }, 2_000)
    assert module.closed == ("BTCUSDT", "BTC", Decimal("2"))
    _, checks = module.get_latest_round_checks()
    assert checks[0].profit_ratio == "0.2"
    assert checks[0].triggered is True
    assert module.recent_action_records()[0].close_order_id == "order-1"


def test_hard_take_profit_does_not_depend_on_other_module_state(tmp_path):
    module = FakeHardTakeProfit(str(tmp_path / "trading.db"))
    module.init_tables()

    assert not module._evaluate_position({
        "symbol": "ETHUSDT", "positionAmt": "3", "entryPrice": "100",
        "unRealizedProfit": "59.99",
    }, 2_000)
    assert not hasattr(module, "closed")
    assert module.get_latest_round_checks()[1][0].close_status == "not_required"


def test_production_database_routing_separates_checks_and_records(tmp_path, monkeypatch):
    trading = tmp_path / "trading.db"
    core = tmp_path / "trading_core.db"
    info = tmp_path / "trading_info.db"
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", str(trading))
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", str(core))
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", str(info))

    module = FakeHardTakeProfit(str(trading))
    module.init_tables()
    module._evaluate_position({
        "symbol": "SOLUSDT", "positionAmt": "1", "entryPrice": "100",
        "unRealizedProfit": "20",
    }, 2_000)

    with sqlite3.connect(core) as conn:
        assert conn.execute("SELECT count(*) FROM hard_take_profit_checks").fetchone()[0] == 1
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='hard_take_profit_records'").fetchone() is None
    with sqlite3.connect(info) as conn:
        assert conn.execute("SELECT count(*) FROM hard_take_profit_records").fetchone()[0] == 1
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='hard_take_profit_checks'").fetchone() is None
