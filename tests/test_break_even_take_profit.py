import tempfile
from pathlib import Path

from trading_experiment import TradingExperiment
from break_even_take_profit import BreakEvenTakeProfitStrategy


class FakeAccountManager:
    def __init__(self):
        self.signed_deletes = []
        self.signed_posts = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5100"}]
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {
                    "symbol": "BANKUSDT",
                    "positionAmt": "2",
                    "entryPrice": "10",
                    "markPrice": "11",
                    "unRealizedProfit": "20",
                    "leverage": "5",
                    "notional": "22",
                    "liquidationPrice": "0",
                }
            ]
        if endpoint == "/fapi/v1/openAlgoOrders":
            return [
                {
                    "symbol": "BANKUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "status": "NEW",
                    "algoId": "123",
                    "triggerPrice": getattr(self, "current_stop_price", "9"),
                }
            ]
        if endpoint == "/fapi/v1/openOrders":
            return []
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        ],
                    }
                ]
            }
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"algoId": params["algoId"], "status": "CANCELED"}

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"algoId": 456}


def test_break_even_strategy_moves_stop_loss_to_entry_when_unrealized_pnl_reaches_r():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        strategy.init_tables()
        experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
        experiment.init_tables()
        with experiment._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TradingExperiment.TRADES_TABLE}
                (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                 required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                 notional_usdt, take_profit_price, stop_loss_price, take_profit_order_id,
                 stop_loss_order_id, reason, raw_response, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("BANK", 1, "LONG", "opened", 80, 5, "20", "4", "1100", "11", "10", "2", "20", "12", "9", "999", "123", "test", "", 1, 1),
            )

        result = strategy.run_round()
        checks = strategy.recent_checks()
        records = strategy.recent_records()

    assert result["checked"] == 1
    assert result["triggered"] == 1
    assert checks[0].triggered is True
    assert checks[0].r_usdt == "11"
    assert fake_account.signed_deletes == [("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "123"})]
    assert fake_account.signed_posts == [
        (
            "/fapi/v1/algoOrder",
            {
                "symbol": "BANKUSDT",
                "side": "SELL",
                "type": "STOP",
                "quantity": "2",
                "price": "10",
                "triggerPrice": "10",
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
                "algoType": "CONDITIONAL",
            },
        )
    ]
    assert records[0].new_stop_loss_order_id == "456"
    assert records[0].stop_loss_price == "10"


def test_break_even_strategy_skips_when_current_stop_loss_is_already_at_entry():
    fake_account = FakeAccountManager()
    fake_account.current_stop_price = "10"
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        strategy.init_tables()
        experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
        experiment.init_tables()
        with experiment._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TradingExperiment.TRADES_TABLE}
                (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                 required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                 notional_usdt, take_profit_price, stop_loss_price, take_profit_order_id,
                 stop_loss_order_id, reason, raw_response, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("BANK", 1, "LONG", "opened", 80, 5, "20", "4", "1100", "11", "10", "2", "20", "12", "9", "999", "123", "test", "", 1, 1),
            )

        result = strategy.run_round()
        checks = strategy.recent_checks()
        records = strategy.recent_records()

    assert result["checked"] == 1
    assert result["triggered"] == 0
    assert result["records"] == 0
    assert checks[0].triggered is True
    assert checks[0].reason == "break_even_already_completed"
    assert fake_account.signed_deletes == []
    assert fake_account.signed_posts == []
    assert records == []


def test_break_even_strategy_cancels_regular_stop_loss_with_regular_order_endpoint():
    fake_account = FakeAccountManager()

    def signed_get(endpoint, params=None):
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5100"}]
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {
                    "symbol": "BANKUSDT",
                    "positionAmt": "2",
                    "entryPrice": "10",
                    "unRealizedProfit": "20",
                }
            ]
        if endpoint == "/fapi/v1/openAlgoOrders":
            return []
        if endpoint == "/fapi/v1/openOrders":
            return [
                {
                    "symbol": "BANKUSDT",
                    "side": "SELL",
                    "type": "STOP_MARKET",
                    "status": "NEW",
                    "orderId": "321",
                    "stopPrice": "9",
                }
            ]
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def signed_delete(endpoint, params=None):
        fake_account.signed_deletes.append((endpoint, dict(params or {})))
        return {"orderId": params["orderId"], "status": "CANCELED"}

    fake_account._signed_get = signed_get
    fake_account._signed_delete = signed_delete

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        result = strategy.run_round()

    assert result["triggered"] == 1
    assert fake_account.signed_deletes == [("/fapi/v1/order", {"symbol": "BANKUSDT", "orderId": "321"})]


def test_break_even_strategy_does_not_retry_algo_stop_loss_cancel_as_regular_order():
    fake_account = FakeAccountManager()

    def signed_delete(endpoint, params=None):
        fake_account.signed_deletes.append((endpoint, dict(params or {})))
        raise RuntimeError("algo cancel failed")

    fake_account._signed_delete = signed_delete

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        result = strategy.run_round()
        records = strategy.recent_records()

    assert result["triggered"] == 1
    assert fake_account.signed_deletes == [("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "123"})]
    assert fake_account.signed_posts == []
    assert records[0].status == "failed"
    assert "break_even_stop_loss_failed" in records[0].reason


def test_break_even_strategy_skips_stale_db_only_stop_loss_cancel_and_creates_new_stop_loss():
    fake_account = FakeAccountManager()

    def signed_get(endpoint, params=None):
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5100"}]
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {
                    "symbol": "BANKUSDT",
                    "positionAmt": "2",
                    "entryPrice": "10",
                    "unRealizedProfit": "20",
                }
            ]
        if endpoint in {"/fapi/v1/openAlgoOrders", "/fapi/v1/openOrders"}:
            return []
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    fake_account._signed_get = signed_get

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        strategy.init_tables()
        experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
        experiment.init_tables()
        with experiment._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TradingExperiment.TRADES_TABLE}
                (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                 required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                 notional_usdt, take_profit_price, stop_loss_price, take_profit_order_id,
                 stop_loss_order_id, reason, raw_response, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("BANK", 1, "LONG", "opened", 80, 5, "20", "4", "1100", "11", "10", "2", "20", "12", "9", "999", "1000000106055690", "test", "", 1, 1),
            )

        result = strategy.run_round()
        records = strategy.recent_records()

    assert result["triggered"] == 1
    assert fake_account.signed_deletes == []
    assert fake_account.signed_posts == [
        (
            "/fapi/v1/algoOrder",
            {
                "symbol": "BANKUSDT",
                "side": "SELL",
                "type": "STOP",
                "quantity": "2",
                "price": "10",
                "triggerPrice": "10",
                "timeInForce": "GTC",
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
                "algoType": "CONDITIONAL",
            },
        )
    ]
    assert records[0].status == "submitted"
    assert records[0].old_stop_loss_order_id == "1000000106055690"
    assert records[0].new_stop_loss_order_id == "456"
    assert "db_stop_loss_order_id_not_open_skip_cancel" in records[0].reason


def test_break_even_strategy_recognizes_open_algo_order_order_type_field():
    fake_account = FakeAccountManager()

    def signed_get(endpoint, params=None):
        rows = FakeAccountManager._signed_get(fake_account, endpoint, params)
        if endpoint == "/fapi/v1/openAlgoOrders":
            rows[0].pop("type")
            rows[0]["orderType"] = "STOP_MARKET"
        return rows

    fake_account._signed_get = signed_get

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        result = strategy.run_round()

    assert result["triggered"] == 1
    assert fake_account.signed_deletes == [("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "123"})]


def test_break_even_strategy_records_failure_when_new_stop_loss_response_has_no_order_id():
    fake_account = FakeAccountManager()

    def signed_post(endpoint, params=None):
        fake_account.signed_posts.append((endpoint, dict(params or {})))
        return {"status": "NEW"}

    fake_account._signed_post = signed_post

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        strategy = BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=fake_account)
        result = strategy.run_round()
        records = strategy.recent_records()

    assert result["triggered"] == 1
    assert fake_account.signed_deletes == [("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "123"})]
    assert len(fake_account.signed_posts) == 1
    assert records[0].status == "failed"
    assert records[0].new_stop_loss_order_id == ""
    assert "break_even_stop_loss_order_id_missing" in records[0].reason
    assert "new_stop_loss" in records[0].raw_response
