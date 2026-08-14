import tempfile
import sqlite3
import pytest
from decimal import Decimal
from pathlib import Path

import db_config
from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment


class FakeAccountManager:
    def __init__(self, unrealized_profit="25", open_stop_price=None):
        self.unrealized_profit = unrealized_profit
        self.open_stop_price = open_stop_price
        self.signed_deletes = []
        self.signed_posts = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/openAlgoOrders":
            if self.open_stop_price is None:
                return []
            return [{
                "symbol": "BANKUSDT", "side": "SELL", "type": "STOP_MARKET",
                "status": "NEW", "triggerPrice": self.open_stop_price, "algoId": 111,
            }]
        if endpoint == "/fapi/v1/openOrders":
            return []
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5100"}]
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {
                    "symbol": "BANKUSDT",
                    "positionAmt": "10",
                    "entryPrice": "10",
                    "markPrice": "12.5",
                    "unRealizedProfit": self.unrealized_profit,
                    "leverage": "5",
                    "notional": "125",
                    "liquidationPrice": "0",
                }
            ]
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/price":
            return {"price": "12.5"}
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "0.1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        ],
                    }
                ]
            }
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"status": "CANCELED"}

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/algoOrder":
            return {"algoId": 456}
        return {"orderId": 789}


def _insert_open_trade(db_path):
    import sqlite3

    TradingExperiment(db_path=db_path, account_manager=FakeAccountManager()).init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
             required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
             notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
             take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
            VALUES ('BANK', 1000, 'LONG', 'opened', 80, 5, '100', '20', '1100', '11',
                    '10', '10', '100', '0', '9', '', '', '111', 'test', '{{}}', 1000, 1000)
            """
        )


def test_partial_take_profit_sells_30_percent_when_unrealized_pnl_reaches_2r():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        result = strategy.run_round()
        _, checks = strategy.get_latest_round_checks()
        records = strategy.recent_records()

    assert result["checked"] == 1
    assert result["triggered"] == 1
    assert result["records"] == 1
    assert checks[0].triggered is True
    assert checks[0].r_usdt == "11"
    assert checks[0].trigger_r_usdt == "22"
    assert fake_account.signed_deletes == [
        ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}),
    ]
    assert fake_account.signed_posts == [
        (
            "/fapi/v1/algoOrder",
            {
                "symbol": "BANKUSDT",
                "side": "SELL",
                "type": "STOP",
                "quantity": "7",
                "price": "9",
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
                "triggerPrice": "9",
                "algoType": "CONDITIONAL",
            },
        ),
        (
            "/fapi/v1/algoOrder",
            {
                "symbol": "BANKUSDT",
                "side": "SELL",
                "type": "TAKE_PROFIT",
                "quantity": "7",
                "price": "17.86",
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
                "triggerPrice": "17.86",
                "algoType": "CONDITIONAL",
            },
        ),
        (
            "/fapi/v1/order",
            {"symbol": "BANKUSDT", "side": "SELL", "quantity": "3", "reduceOnly": "true", "type": "MARKET", "newOrderRespType": "RESULT"},
        )
    ]
    assert records[0].take_profit_order_id == "789"
    assert records[0].take_profit_quantity == "3"
    assert records[0].trigger_label == "已触发2R分批止盈"


def test_partial_take_profit_updates_trade_in_routed_core_database(monkeypatch, tmp_path):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    info_db = str(tmp_path / "trading_info.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", info_db)
    fake_account = FakeAccountManager()
    experiment = TradingExperiment(db_path=trading_db, account_manager=fake_account)
    experiment.init_tables()
    with experiment._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
             required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
             notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
             take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
            VALUES ('BANK', 1000, 'LONG', 'opened', 80, 5, '100', '20', '1100', '11',
                    '10', '10', '100', '0', '9', '', '', '111', 'test', '{{}}', 1000, 1000)
            """
        )

    strategy = PartialTakeProfitStrategy(db_path=trading_db, account_manager=fake_account)
    result = strategy.run_round()

    assert result["triggered"] == 1
    assert strategy.recent_records()[0].status == "submitted"
    with sqlite3.connect(core_db) as conn:
        row = conn.execute(
            f"SELECT take_profit_order_id, stop_loss_order_id FROM {TradingExperiment.TRADES_TABLE}"
        ).fetchone()
    assert row == ("456", "456")
    with sqlite3.connect(trading_db) as conn:
        trade_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TradingExperiment.TRADES_TABLE,),
        ).fetchone()
    assert trade_table is None


def test_partial_take_profit_initializes_routed_core_trade_table(monkeypatch, tmp_path):
    trading_db = str(tmp_path / "trading.db")
    core_db = str(tmp_path / "trading_core.db")
    info_db = str(tmp_path / "trading_info.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", core_db)
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", info_db)

    strategy = PartialTakeProfitStrategy(
        db_path=trading_db, account_manager=FakeAccountManager()
    )
    strategy.init_tables()

    with sqlite3.connect(core_db) as conn:
        core_trade_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TradingExperiment.TRADES_TABLE,),
        ).fetchone()
    with sqlite3.connect(trading_db) as conn:
        trading_trade_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TradingExperiment.TRADES_TABLE,),
        ).fetchone()

    assert core_trade_table == (1,)
    assert trading_trade_table is None


def test_partial_take_profit_skips_when_unrealized_pnl_below_2r():
    fake_account = FakeAccountManager(unrealized_profit="19.99")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        result = strategy.run_round()
        _, checks = strategy.get_latest_round_checks()
        records = strategy.recent_records()

    assert result["checked"] == 1
    assert result["triggered"] == 0
    assert result["records"] == 0
    assert checks[0].reason == "unrealized_pnl_lt_2r"
    assert records == []
    assert fake_account.signed_posts == []


def test_partial_take_profit_persists_1_4r_trigger_label():
    with tempfile.TemporaryDirectory() as tmpdir:
        strategy = PartialTakeProfitStrategy(
            db_path=str(Path(tmpdir) / "klines.db"),
            account_manager=FakeAccountManager(),
        )
        strategy.init_tables()
        strategy._insert_record(
            "BANK", 1000, "SELL", Decimal("10"), Decimal("5"), Decimal("10"),
            Decimal("1100"), Decimal("11"), Decimal("15.4"), Decimal("16"),
            "789", "submitted", "weak_market_partial_take_profit", "{}", Decimal("1.4"),
        )

        records = strategy.recent_records()

    assert records[0].trigger_label == "已触发1.4R分批止盈"


def test_partial_take_profit_preserves_live_break_even_stop_price():
    fake_account = FakeAccountManager(open_stop_price="10")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        strategy.run_round()
        record = strategy.recent_records()[0]

    stop_order = next(params for endpoint, params in fake_account.signed_posts if params.get("type") == "STOP")
    assert stop_order["triggerPrice"] == "10"
    assert record.status == "submitted"
    assert "remaining_stop_loss_price_from_live_order" in record.reason


def test_partial_take_profit_uses_live_stop_when_trade_price_is_missing():
    fake_account = FakeAccountManager(open_stop_price="10")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(f"UPDATE {TradingExperiment.TRADES_TABLE} SET stop_loss_price = '0'")
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        strategy.run_round()
        record = strategy.recent_records()[0]

    assert record.status == "submitted"
    assert record.take_profit_quantity == "3"


def test_partial_take_profit_does_not_cancel_orders_when_no_stop_price_exists():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(f"UPDATE {TradingExperiment.TRADES_TABLE} SET stop_loss_price = '0'")
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        strategy.run_round()
        record = strategy.recent_records()[0]

    assert record.status == "failed"
    assert "remaining_stop_loss_not_recreated_missing_price" in record.reason
    assert fake_account.signed_deletes == []
    assert fake_account.signed_posts == []


def test_failed_trade_attempt_is_authoritative_error_source():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(f"UPDATE {TradingExperiment.TRADES_TABLE} SET stop_loss_price = '0'")
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        strategy.run_round(decision_round_ts=1234)
        errors = strategy.recent_errors()

    assert len(errors) == 1
    assert errors[0]["source"] == PartialTakeProfitStrategy.RECORDS_TABLE
    assert errors[0]["stage"] == "trade_attempt"
    assert "remaining_stop_loss_not_recreated_missing_price" in errors[0]["error_message"]


def test_error_before_trade_attempt_uses_dedicated_error_table(monkeypatch):
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        def fail_evaluation(*args, **kwargs):
            raise RuntimeError("evaluation exploded")

        monkeypatch.setattr(strategy, "_evaluate_position", fail_evaluation)
        result = strategy.run_round(decision_round_ts=1234)
        errors = strategy.recent_errors()

    assert result["checked"] == 1
    assert result["records"] == 0
    assert len(errors) == 1
    assert errors[0]["source"] == PartialTakeProfitStrategy.ERRORS_TABLE
    assert errors[0]["stage"] == "evaluate_position"
    assert errors[0]["symbol"] == "BANK"
    assert errors[0]["error_message"] == "evaluation exploded"


def test_round_error_before_position_scan_is_persisted():
    class InvalidAccountManager(FakeAccountManager):
        def validate_config(self):
            raise ValueError("invalid credentials")

    with tempfile.TemporaryDirectory() as tmpdir:
        strategy = PartialTakeProfitStrategy(
            db_path=str(Path(tmpdir) / "klines.db"),
            account_manager=InvalidAccountManager(),
        )
        with pytest.raises(ValueError, match="invalid credentials"):
            strategy.run_round(decision_round_ts=1234)
        errors = strategy.recent_errors()

    assert errors[0]["source"] == PartialTakeProfitStrategy.ERRORS_TABLE
    assert errors[0]["stage"] == "validate_config"
    assert errors[0]["decision_round_ts"] == 1234
