import tempfile
from pathlib import Path

from partial_take_profit import PartialTakeProfitStrategy


class FakeAccountManager:
    def __init__(self, unrealized_profit="25"):
        self.unrealized_profit = unrealized_profit
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

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 789}


def test_partial_take_profit_sells_30_percent_when_unrealized_pnl_reaches_2r():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
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
    assert fake_account.signed_posts == [
        (
            "/fapi/v1/order",
            {"symbol": "BANKUSDT", "side": "SELL", "type": "MARKET", "quantity": "3", "reduceOnly": "true"},
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
