import tempfile
from pathlib import Path

from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment


class FakeAccountManager:
    def __init__(self, unrealized_profit="25"):
        self.unrealized_profit = unrealized_profit
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
            "/fapi/v1/order",
            {"symbol": "BANKUSDT", "side": "SELL", "quantity": "3", "reduceOnly": "true", "type": "MARKET", "newOrderRespType": "RESULT"},
        )
    ]
    assert records[0].take_profit_order_id == "789"
    assert records[0].take_profit_quantity == "3"


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
