import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from openable_symbol_module import OpenableSymbol
from trading_experiment import TradingExperiment


class FakeAccountManager:
    def __init__(self):
        self.signed_posts = []

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                        ],
                    }
                ]
            }
        if endpoint == "/fapi/v1/ticker/price":
            self.latest_price_params = params
            return {"price": "1"}
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": len(self.signed_posts)}


class TradingExperimentSymbolTests(unittest.TestCase):
    def test_base_symbol_is_expanded_to_binance_usdt_pair_for_order_api_calls(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            experiment.init_tables()
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=80,
                score_band="标准试错单",
                stop_loss_distance_ratio=0.01,
                distance_threshold=0.02,
                stop_loss_distance_tier="A档",
                opening_leverage="4x",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            result = experiment._open_long(candidate, Decimal("1000"), Decimal("10"))

        self.assertEqual(result["status"], "opened")
        self.assertEqual(fake_account.latest_price_params, {"symbol": "BANKUSDT"})
        symbol_params = [params["symbol"] for _, params in fake_account.signed_posts]
        self.assertEqual(symbol_params, ["BANKUSDT", "BANKUSDT", "BANKUSDT", "BANKUSDT"])

    def test_usdt_pair_symbol_is_not_double_suffixed(self):
        self.assertEqual(TradingExperiment._binance_symbol("BANKUSDT"), "BANKUSDT")


if __name__ == "__main__":
    unittest.main()
