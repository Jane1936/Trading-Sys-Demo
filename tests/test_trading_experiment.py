import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from openable_symbol_module import OpenableSymbol
from trading_experiment import ExperimentConfig, TradingExperiment


class FakeAccountManager:
    def __init__(self):
        self.signed_posts = []

    def validate_config(self):
        return None

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
        if endpoint == "/fapi/v1/premiumIndex":
            self.latest_mark_price_params = params
            return {"markPrice": "1"}
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/algoOrder":
            return {"algoId": len(self.signed_posts)}
        return {"orderId": len(self.signed_posts)}

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/account":
            return {"availableBalance": "1000", "totalMarginBalance": "1000"}
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5000"}]
        if endpoint == "/fapi/v3/positionRisk":
            if params and "symbol" in params:
                return [{"symbol": params["symbol"], "positionAmt": "50"}]
            return []
        raise AssertionError(f"unexpected signed endpoint {endpoint}")


class CoarseLotAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "7"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                        ],
                    }
                ]
            }
        return super()._public_get(endpoint, params)


class FailingTakeProfitAccountManager(FakeAccountManager):
    def _signed_post(self, endpoint, params=None):
        if params and params.get("type") == "TAKE_PROFIT_MARKET":
            raise RuntimeError("take profit failed")
        return super()._signed_post(endpoint, params)

class EmptyTickerAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/price":
            self.latest_price_params = params
            return {}
        if endpoint == "/fapi/v1/premiumIndex":
            self.latest_mark_price_params = params
            return {"markPrice": "2"}
        return super()._public_get(endpoint, params)


class InvalidPriceAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/price":
            return {}
        if endpoint == "/fapi/v1/premiumIndex":
            return {}
        return super()._public_get(endpoint, params)


class MissingPositionOnceAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.take_profit_failures = 0
        self.position_risk_requests = []

    def _signed_post(self, endpoint, params=None):
        if (
            endpoint == "/fapi/v1/algoOrder"
            and params
            and params.get("type") == "TAKE_PROFIT_MARKET"
            and self.take_profit_failures == 0
        ):
            self.take_profit_failures += 1
            self.signed_posts.append((endpoint, dict(params or {})))
            raise RuntimeError(
                "400 Client Error: Bad Request response_body="
                '{"code":-4509,"msg":"Time in Force (TIF) GTE can only be used with open positions. '
                'Please ensure that positions are available."}'
            )
        return super()._signed_post(endpoint, params)

    def _signed_get(self, endpoint, params=None):
        self.position_risk_requests.append((endpoint, dict(params or {})))
        return super()._signed_get(endpoint, params)


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
        self.assertEqual(fake_account.latest_mark_price_params, {"symbol": "BANKUSDT"})
        symbol_params = [params["symbol"] for _, params in fake_account.signed_posts]
        self.assertEqual(symbol_params, ["BANKUSDT", "BANKUSDT", "BANKUSDT", "BANKUSDT"])
        endpoints = [endpoint for endpoint, _ in fake_account.signed_posts]
        self.assertEqual(
            endpoints,
            ["/fapi/v1/leverage", "/fapi/v1/order", "/fapi/v1/algoOrder", "/fapi/v1/algoOrder"],
        )
        order_types = [params.get("type", "LEVERAGE") for _, params in fake_account.signed_posts]
        self.assertEqual(order_types, ["LEVERAGE", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"])
        self.assertEqual(fake_account.signed_posts[1][1]["quantity"], "1000")
        stop_loss_params = fake_account.signed_posts[2][1]
        take_profit_params = fake_account.signed_posts[3][1]
        self.assertEqual(stop_loss_params["algoType"], "CONDITIONAL")
        self.assertEqual(take_profit_params["algoType"], "CONDITIONAL")
        self.assertEqual(stop_loss_params["triggerPrice"], "0.99")
        self.assertEqual(take_profit_params["triggerPrice"], "1.2")
        self.assertNotIn("stopPrice", stop_loss_params)
        self.assertNotIn("stopPrice", take_profit_params)

    def test_latest_price_falls_back_to_mark_price_when_ticker_payload_is_empty(self):
        fake_account = EmptyTickerAccountManager()
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
        self.assertEqual(fake_account.latest_mark_price_params, {"symbol": "BANKUSDT"})
        self.assertEqual(fake_account.signed_posts[1][1]["quantity"], "500")

    def test_run_round_skips_candidate_when_latest_price_is_unavailable(self):
        fake_account = InvalidPriceAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
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

            result = experiment.run_round([candidate])
            error_rows = experiment.recent_error_records()

        self.assertEqual(result, {"opened": 0, "skipped": 1, "reason": "completed"})
        self.assertEqual(fake_account.signed_posts, [])
        self.assertEqual(error_rows[0].operation, "open_long")

    def test_stop_loss_price_uses_equity_risk_per_coin_after_quantity_rounding(self):
        fake_account = CoarseLotAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            experiment.init_tables()
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=89,
                score_band="确定性强趋势单",
                stop_loss_distance_ratio=0.02,
                distance_threshold=0.08,
                stop_loss_distance_tier="A档",
                opening_leverage="10x",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            experiment._open_long(candidate, Decimal("1000"), Decimal("10"))

        order_params = fake_account.signed_posts[1][1]
        stop_loss_params = fake_account.signed_posts[2][1]
        self.assertEqual(order_params["quantity"], "497")
        self.assertEqual(stop_loss_params["triggerPrice"], "0.979879")

    def test_recent_trade_records_only_returns_opened_rows(self):
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
            experiment._record_skip(candidate, Decimal("1000"), Decimal("10"), "test_skip")
            experiment._open_long(candidate, Decimal("1000"), Decimal("10"))

            rows = experiment.recent_trade_records()

        self.assertEqual([row.status for row in rows], ["opened"])

    def test_take_profit_failure_does_not_hide_opened_trade_or_stop_loss_order(self):
        fake_account = FailingTakeProfitAccountManager()
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
            trade_rows = experiment.recent_trade_records()
            error_rows = experiment.recent_error_records()

        self.assertEqual(result["status"], "opened")
        self.assertEqual(trade_rows[0].status, "opened")
        self.assertEqual(trade_rows[0].take_profit_order_id, "")
        self.assertEqual(trade_rows[0].stop_loss_order_id, "3")
        self.assertEqual(error_rows[0].operation, "place_take_profit:BANKUSDT")

    def test_exit_order_retries_missing_position_algo_order_error(self):
        fake_account = MissingPositionOnceAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
                config=ExperimentConfig(exit_order_missing_position_retry_delay_seconds=Decimal("0")),
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
            trade_rows = experiment.recent_trade_records()
            error_rows = experiment.recent_error_records()

        self.assertEqual(result["status"], "opened")
        self.assertEqual(fake_account.take_profit_failures, 1)
        self.assertEqual(
            fake_account.position_risk_requests,
            [("/fapi/v3/positionRisk", {"symbol": "BANKUSDT"})],
        )
        self.assertEqual(trade_rows[0].take_profit_order_id, "5")
        self.assertEqual(error_rows, [])


    def test_run_round_requires_qualified_candidate_and_positive_leverage(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            candidates = [
                OpenableSymbol(
                    symbol="BANK",
                    decision_round_ts=1,
                    total_score=90,
                    score_band="确定性强趋势单",
                    stop_loss_distance_ratio=0.01,
                    distance_threshold=0.08,
                    stop_loss_distance_tier="A档",
                    opening_leverage="NA",
                    distance_qualified=True,
                    qualified=True,
                    reason="test",
                    evaluated_at=1,
                ),
                OpenableSymbol(
                    symbol="BANK",
                    decision_round_ts=1,
                    total_score=89,
                    score_band="确定性强趋势单",
                    stop_loss_distance_ratio=0.01,
                    distance_threshold=0.08,
                    stop_loss_distance_tier="A档",
                    opening_leverage="4x",
                    distance_qualified=True,
                    qualified=False,
                    reason="test",
                    evaluated_at=1,
                ),
            ]

            result = experiment.run_round(candidates)

        self.assertEqual(result, {"opened": 0, "skipped": 0, "reason": "completed"})
        self.assertEqual(fake_account.signed_posts, [])

    def test_usdt_pair_symbol_is_not_double_suffixed(self):
        self.assertEqual(TradingExperiment._binance_symbol("BANKUSDT"), "BANKUSDT")

    def test_trade_plan_uses_experiment_equity_distance_and_leverage(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=89,
                score_band="确定性强趋势单",
                stop_loss_distance_ratio=0.02,
                distance_threshold=0.08,
                stop_loss_distance_tier="A档",
                opening_leverage="10x",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            equity = experiment._fetch_experiment_usdt_equity()
            plan = experiment._trade_plan(candidate, equity)

        self.assertEqual(equity, Decimal("1000"))
        self.assertEqual(plan.required_margin_usdt, Decimal("50"))
        self.assertEqual(plan.planned_notional_usdt, Decimal("500"))

    def test_trade_plan_defaults_zero_distance_trend_candidate_to_five_percent_and_5x(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=81,
                score_band="趋势标准单",
                stop_loss_distance_ratio=0,
                distance_threshold=0.07,
                stop_loss_distance_tier="NA",
                opening_leverage="NA",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            plan = experiment._trade_plan(candidate, Decimal("1000"))

        self.assertTrue(TradingExperiment._candidate_allows_open(candidate))
        self.assertEqual(plan.leverage, 5)
        self.assertEqual(plan.stop_loss_distance_ratio, Decimal("0.05"))
        self.assertEqual(plan.required_margin_usdt, Decimal("40"))
        self.assertEqual(plan.planned_notional_usdt, Decimal("200"))


if __name__ == "__main__":
    unittest.main()
