import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from openable_symbol_module import OpenableSymbol, OpenableSymbolModule
from trading_experiment import ExperimentConfig, TradingExperiment

class FakeAccountManager:
    def __init__(self):
        self.signed_posts = []
        self.signed_deletes = []

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

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"code": 200, "msg": "success"}

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

class TenExistingPositionsAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            symbols = [
                {
                    "symbol": f"COIN{i}USDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "1"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                    ],
                }
                for i in range(10)
            ]
            symbols.append(
                {
                    "symbol": "BANKUSDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "1"},
                        {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                    ],
                }
            )
            return {"symbols": symbols}
        return super()._public_get(endpoint, params)

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and not params:
            return [{"symbol": f"COIN{i}USDT", "positionAmt": "1", "leverage": "1"} for i in range(10)]
        return super()._signed_get(endpoint, params)

class ExistingBankPositionAccountManager(FakeAccountManager):
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
                    },
                    {
                        "symbol": "COINUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                        ],
                    },
                ]
            }
        return super()._public_get(endpoint, params)

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and not params:
            return [{"symbol": "BANKUSDT", "positionAmt": "3", "leverage": "5"}]
        return super()._signed_get(endpoint, params)

class ProfitableExperimentBudgetAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/account":
            return {"availableBalance": "1200", "totalMarginBalance": "1200"}
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5200"}]
        if endpoint == "/fapi/v3/positionRisk" and not params:
            return [{"symbol": "OLDUSDT", "positionAmt": "1100", "notional": "1100", "leverage": "1"}]
        return super()._signed_get(endpoint, params)


class EquityBelowUsedMarginAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "4282"}]
        if endpoint == "/fapi/v3/positionRisk" and not params:
            return [{"symbol": "OLDUSDT", "positionAmt": "1204.75", "notional": "1204.75", "leverage": "1"}]
        return super()._signed_get(endpoint, params)

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

class InvalidSymbolOrderAccountManager(FakeAccountManager):
    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/order":
            raise RuntimeError(
                "400 Client Error: Bad Request response_body="
                '{"code":-1121,"msg":"Invalid symbol."}'
            )
        return {"orderId": len(self.signed_posts)}

class InvalidLeverageAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "STABLEUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                        ],
                    },
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001"},
                        ],
                    },
                ]
            }
        return super()._public_get(endpoint, params)

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/leverage" and params and params.get("symbol") == "STABLEUSDT":
            raise RuntimeError(
                "400 Client Error: Bad Request response_body="
                '{"code":-4028,"msg":"Leverage 7 is not valid"}'
            )
        if endpoint == "/fapi/v1/algoOrder":
            return {"algoId": len(self.signed_posts)}
        return {"orderId": len(self.signed_posts)}

class PositionEntryPriceAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.00001"},
                        ],
                    }
                ]
            }
        if endpoint == "/fapi/v1/ticker/price":
            self.latest_price_params = params
            return {"price": "0.00856"}
        if endpoint == "/fapi/v1/premiumIndex":
            self.latest_mark_price_params = params
            return {"markPrice": "0.00857934"}
        return super()._public_get(endpoint, params)

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and params and "symbol" in params:
            return [{"symbol": params["symbol"], "positionAmt": "70867", "entryPrice": "0.00864"}]
        return super()._signed_get(endpoint, params)

class DelayedPositionAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.position_risk_requests = []

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and params and "symbol" in params:
            self.position_risk_requests.append((endpoint, dict(params or {})))
            if len(self.position_risk_requests) < 3:
                return [{"symbol": params["symbol"], "positionAmt": "0"}]
            return [{"symbol": params["symbol"], "positionAmt": "50"}]
        return super()._signed_get(endpoint, params)

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

class HighRiskAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and not params:
            return [
                {
                    "symbol": "OTHERUSDT",
                    "positionAmt": "5",
                    "entryPrice": "1000",
                    "markPrice": "1000",
                    "unRealizedProfit": "0",
                    "leverage": "4",
                    "notional": "3000",
                    "liquidationPrice": "0",
                }
            ]
        return super()._signed_get(endpoint, params)

class TradingExperimentSymbolTests(unittest.TestCase):
    def test_base_symbol_is_expanded_to_binance_usdt_pair_for_order_api_calls(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "klines.db"
            experiment = TradingExperiment(
                db_path=str(db_path),
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
        self.assertEqual(order_types, ["LEVERAGE", "MARKET", "STOP", "TAKE_PROFIT"])
        self.assertEqual(fake_account.signed_posts[1][1]["quantity"], "1000")
        stop_loss_params = fake_account.signed_posts[2][1]
        self.assertEqual(stop_loss_params["type"], "STOP")
        self.assertEqual(stop_loss_params["price"], "0.99")
        self.assertEqual(stop_loss_params["triggerPrice"], "0.99")
        self.assertEqual(stop_loss_params["algoType"], "CONDITIONAL")
        self.assertEqual(stop_loss_params["timeInForce"], "GTC")
        self.assertEqual(stop_loss_params["reduceOnly"], "true")
        self.assertEqual(trade_rows[0].take_profit_price, "1.055")
        self.assertEqual(trade_rows[0].take_profit_order_id, "4")
        self.assertIn("max_loss=10", trade_rows[0].stop_loss_calculation)
        self.assertIn("risk_distance=max_loss/quantity=0.01", trade_rows[0].stop_loss_calculation)
        self.assertIn("final_stop_loss_price=floor_to_tick(raw_stop_price)=0.99", trade_rows[0].stop_loss_calculation)
        self.assertIn("tp_order_id=4", trade_rows[0].reason)

    def test_open_long_waits_until_position_amt_is_positive_before_exit_orders(self):
        fake_account = DelayedPositionAccountManager()
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

        self.assertEqual(result["status"], "opened")
        self.assertEqual(
            fake_account.position_risk_requests,
            [
                ("/fapi/v3/positionRisk", {"symbol": "BANKUSDT"}),
                ("/fapi/v3/positionRisk", {"symbol": "BANKUSDT"}),
                ("/fapi/v3/positionRisk", {"symbol": "BANKUSDT"}),
            ],
        )
        endpoints = [endpoint for endpoint, _ in fake_account.signed_posts]
        self.assertEqual(endpoints[:2], ["/fapi/v1/leverage", "/fapi/v1/order"])
        self.assertEqual(endpoints[2:], ["/fapi/v1/algoOrder", "/fapi/v1/algoOrder"])

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

    def test_run_round_allows_low_score_new_entries_when_current_total_risk_exceeds_eighteen(self):
        fake_account = HighRiskAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "klines.db"
            experiment = TradingExperiment(
                db_path=str(db_path),
                account_manager=fake_account,
            )
            experiment.init_tables()
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE klines_15m (symbol TEXT NOT NULL, open_time INTEGER NOT NULL, close REAL NOT NULL)")
                conn.execute("INSERT INTO klines_15m (symbol, open_time, close) VALUES ('OTHER', 1, 1100)")
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
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                trade_row = conn.execute(
                    f"SELECT status, reason, required_margin_usdt FROM {experiment.TRADES_TABLE} ORDER BY id DESC LIMIT 1"
                ).fetchone()

        self.assertEqual(result, {"opened": 1, "skipped": 0, "reason": "completed"})
        self.assertEqual(trade_row["status"], "opened")
        self.assertIn("market_order_id=", trade_row["reason"])
        self.assertEqual(trade_row["required_margin_usdt"], "250")
        self.assertEqual(
            [endpoint for endpoint, _ in fake_account.signed_posts],
            ["/fapi/v1/leverage", "/fapi/v1/order", "/fapi/v1/algoOrder", "/fapi/v1/algoOrder"],
        )

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

    def test_run_round_skips_candidate_when_demo_order_rejects_invalid_symbol(self):
        fake_account = InvalidSymbolOrderAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "klines.db"
            experiment = TradingExperiment(
                db_path=str(db_path),
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
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                trade_row = conn.execute(
                    f"SELECT status, reason FROM {experiment.TRADES_TABLE} ORDER BY id DESC LIMIT 1"
                ).fetchone()

        self.assertEqual(result, {"opened": 0, "skipped": 1, "reason": "completed"})
        self.assertEqual([params.get("type", "LEVERAGE") for _, params in fake_account.signed_posts], ["LEVERAGE", "MARKET"])
        self.assertEqual(trade_row["status"], "skipped")
        self.assertEqual(trade_row["reason"], "invalid_binance_symbol")
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
            rows = experiment.recent_trade_records()

        order_params = fake_account.signed_posts[1][1]
        stop_loss_params = fake_account.signed_posts[2][1]
        self.assertEqual(order_params["quantity"], "497")
        self.assertEqual(stop_loss_params["type"], "STOP")
        self.assertEqual(stop_loss_params["price"], "0.979879")
        self.assertEqual(stop_loss_params["triggerPrice"], "0.979879")
        self.assertEqual(stop_loss_params["algoType"], "CONDITIONAL")
        self.assertIn("quantity=497", rows[0].stop_loss_calculation)
        self.assertIn("risk_distance=max_loss/quantity=0.02012072434607645875251509054", rows[0].stop_loss_calculation)
        self.assertIn("final_stop_loss_price=floor_to_tick(raw_stop_price)=0.979879", rows[0].stop_loss_calculation)

    def test_percent_price_market_rejection_retries_as_limit_ioc_with_one_percent_slippage(self):
        class PercentPriceRetryAccountManager(FakeAccountManager):
            def _signed_post(self, endpoint, params=None):
                self.signed_posts.append((endpoint, dict(params or {})))
                if len(self.signed_posts) == 1:
                    raise RuntimeError('HTTPError response_body={"code":-4131,"msg":"PERCENT_PRICE"}')
                return {"orderId": 222}

        fake_account = PercentPriceRetryAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            response = experiment._signed_post_order_with_ioc_retry(
                "/fapi/v1/order",
                {
                    "symbol": "BANKUSDT",
                    "side": "SELL",
                    "type": "MARKET",
                    "quantity": "3",
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                },
            )

        self.assertEqual(response["orderId"], 222)
        self.assertEqual(fake_account.signed_posts[0][1]["type"], "MARKET")
        self.assertEqual(
            fake_account.signed_posts[1],
            (
                "/fapi/v1/order",
                {
                    "symbol": "BANKUSDT",
                    "side": "SELL",
                    "quantity": "3",
                    "reduceOnly": "true",
                    "newOrderRespType": "RESULT",
                    "type": "LIMIT",
                    "timeInForce": "IOC",
                    "price": "0.99",
                },
            ),
        )

    def test_stop_loss_uses_position_entry_price_after_market_fill(self):
        fake_account = PositionEntryPriceAccountManager()
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

            experiment._open_long(candidate, Decimal("1000"), Decimal("7.4220549961"))
            rows = experiment.recent_trade_records()

        stop_loss_params = fake_account.signed_posts[2][1]
        self.assertEqual(stop_loss_params["price"], "0.00853")
        self.assertEqual(stop_loss_params["triggerPrice"], "0.00853")
        self.assertIn("entry_price=0.00864", rows[0].stop_loss_calculation)
        self.assertIn(
            "desired_price=entry_price-selected_distance=0.008535267825700255408017836228",
            rows[0].stop_loss_calculation,
        )
        self.assertIn(
            "final_stop_loss_price=floor_to_tick(raw_stop_price)=0.00853",
            rows[0].stop_loss_calculation,
        )

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

    def test_hard_take_profit_order_is_placed_for_new_opened_trade(self):
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
        self.assertEqual(trade_rows[0].take_profit_price, "1.055")
        self.assertEqual(trade_rows[0].take_profit_order_id, "4")
        self.assertEqual(trade_rows[0].stop_loss_order_id, "3")
        self.assertEqual(error_rows, [])

    def test_exit_order_does_not_place_take_profit_after_stop_loss(self):
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
        self.assertEqual(fake_account.take_profit_failures, 0)
        self.assertEqual(
            fake_account.position_risk_requests,
            [("/fapi/v3/positionRisk", {"symbol": "BANKUSDT"})],
        )
        self.assertEqual(trade_rows[0].take_profit_price, "1.055")
        self.assertEqual(trade_rows[0].take_profit_order_id, "4")
        self.assertEqual(trade_rows[0].stop_loss_order_id, "3")
        self.assertEqual(error_rows, [])

    def test_scheduled_run_latest_round_ignores_stale_openable_round(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            module = OpenableSymbolModule(db_path=db_path)
            module.init_table()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    f"""
                    INSERT INTO {module.TABLE_NAME}
                    (symbol, decision_round_ts, total_score, score_band, stop_loss_distance_ratio,
                     distance_threshold, stop_loss_distance_tier, opening_leverage,
                     distance_qualified, qualified, reason, evaluated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("BANK", 900_000, 90, "确定性强趋势单", 0.01, 0.08, "A档", "4x", 1, 1, "test", 1),
                )

            experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
            result = experiment.run_latest_round(decision_round_ts=1_800_000)

        self.assertEqual(result, {"opened": 0, "skipped": 0, "reason": "current_round_openable_not_ready"})
        self.assertEqual(fake_account.signed_posts, [])

    def test_current_decision_round_ts_uses_15_minute_floor(self):
        self.assertEqual(TradingExperiment.current_decision_round_ts(now_ms=1_830_000), 1_800_000)


    def test_run_round_blocks_new_entries_when_equity_below_used_margin(self):
        fake_account = EquityBelowUsedMarginAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=90,
                score_band="确定性强趋势单",
                stop_loss_distance_ratio=0.10,
                distance_threshold=0.08,
                stop_loss_distance_tier="A档",
                opening_leverage="10x",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            result = experiment.run_round([candidate])
            with sqlite3.connect(experiment.db_path) as conn:
                rows = conn.execute(
                    f"SELECT status, reason FROM {TradingExperiment.TRADES_TABLE} ORDER BY id DESC"
                ).fetchall()

        self.assertEqual(result, {"opened": 0, "skipped": 1, "reason": "completed"})
        self.assertEqual(rows[0], ("skipped", "experiment_equity_below_used_margin_open_increase_blocked"))
        self.assertEqual(fake_account.signed_posts, [])

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

    def test_run_round_skips_invalid_binance_leverage_and_continues_next_symbol(self):
        fake_account = InvalidLeverageAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "klines.db"
            experiment = TradingExperiment(
                db_path=str(db_path),
                account_manager=fake_account,
            )
            candidates = [
                OpenableSymbol(
                    symbol="STABLE",
                    decision_round_ts=1,
                    total_score=90,
                    score_band="确定性强趋势单",
                    stop_loss_distance_ratio=0.01,
                    distance_threshold=0.08,
                    stop_loss_distance_tier="A档",
                    opening_leverage="7x",
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
                    qualified=True,
                    reason="test",
                    evaluated_at=1,
                ),
            ]

            result = experiment.run_round(candidates)
            errors = experiment.recent_error_records()
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    f"SELECT symbol, status, reason FROM {TradingExperiment.TRADES_TABLE} ORDER BY id ASC"
                ).fetchall()

        self.assertEqual(result, {"opened": 1, "skipped": 1, "reason": "completed"})
        self.assertEqual(rows[0], ("STABLE", "skipped", "invalid_binance_leverage"))
        self.assertEqual(rows[1][0:2], ("BANK", "opened"))
        self.assertEqual(errors[0].symbol, "STABLE")
        self.assertIn("-4028", errors[0].error_message)
        leverage_calls = [
            params
            for endpoint, params in fake_account.signed_posts
            if endpoint == "/fapi/v1/leverage"
        ]
        self.assertEqual(
            leverage_calls,
            [
                {"symbol": "STABLEUSDT", "leverage": 7},
                {"symbol": "BANKUSDT", "leverage": 4},
            ],
        )


    def test_existing_position_symbol_is_skipped_and_next_candidate_is_opened(self):
        fake_account = ExistingBankPositionAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
                conn.execute("INSERT INTO klines_15m (symbol, open_time, close) VALUES ('BANK', 1, 1)")
            experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
            candidates = [
                OpenableSymbol(
                    symbol="BANK",
                    decision_round_ts=1,
                    total_score=95,
                    score_band="确定性强趋势单",
                    stop_loss_distance_ratio=0.10,
                    distance_threshold=0.08,
                    stop_loss_distance_tier="A档",
                    opening_leverage="10x",
                    distance_qualified=True,
                    qualified=True,
                    reason="test",
                    evaluated_at=1,
                ),
                OpenableSymbol(
                    symbol="COIN",
                    decision_round_ts=1,
                    total_score=90,
                    score_band="确定性强趋势单",
                    stop_loss_distance_ratio=0.10,
                    distance_threshold=0.08,
                    stop_loss_distance_tier="A档",
                    opening_leverage="10x",
                    distance_qualified=True,
                    qualified=True,
                    reason="test",
                    evaluated_at=1,
                ),
            ]

            result = experiment.run_round(candidates)
            with sqlite3.connect(experiment.db_path) as conn:
                rows = conn.execute(
                    f"SELECT symbol, status, reason FROM {TradingExperiment.TRADES_TABLE} ORDER BY id ASC"
                ).fetchall()

        self.assertEqual(result, {"opened": 1, "skipped": 1, "reason": "completed"})
        self.assertEqual(rows[0], ("BANK", "skipped", "symbol_position_already_open"))
        self.assertEqual(rows[1][0:2], ("COIN", "opened"))
        market_orders = [
            params for endpoint, params in fake_account.signed_posts
            if endpoint == "/fapi/v1/order" and params.get("side") == "BUY"
        ]
        self.assertEqual([row["symbol"] for row in market_orders], ["COINUSDT"])

    def test_opening_is_not_blocked_by_existing_position_count_when_balance_is_enough(self):
        fake_account = TenExistingPositionsAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
                for i in range(10):
                    conn.execute("INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)", (f"COIN{i}", 1, 1))
            experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=90,
                score_band="确定性强趋势单",
                stop_loss_distance_ratio=0.10,
                distance_threshold=0.08,
                stop_loss_distance_tier="A档",
                opening_leverage="10x",
                distance_qualified=True,
                qualified=True,
                reason="test",
                evaluated_at=1,
            )

            result = experiment.run_round([candidate])
            rows = experiment.recent_trade_records()

        self.assertEqual(result, {"opened": 1, "skipped": 0, "reason": "completed"})
        self.assertEqual(rows[0].symbol, "BANK")

    def test_opening_budget_uses_current_experiment_equity_after_profit(self):
        fake_account = ProfitableExperimentBudgetAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            experiment = TradingExperiment(db_path=db_path, account_manager=fake_account)
            candidate = OpenableSymbol(
                symbol="BANK",
                decision_round_ts=1,
                total_score=90,
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

            result = experiment.run_round([candidate])
            rows = experiment.recent_trade_records()

        self.assertEqual(result, {"opened": 1, "skipped": 0, "reason": "completed"})
        self.assertEqual(rows[0].symbol, "BANK")
        self.assertEqual(rows[0].account_equity_usdt, "1200")

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

    def test_trade_plan_does_not_default_zero_distance_trend_candidate(self):
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
                distance_qualified=False,
                qualified=False,
                reason="trend_zero_distance_ratio_not_openable",
                evaluated_at=1,
            )

            plan = experiment._trade_plan(candidate, Decimal("1000"))

        self.assertFalse(TradingExperiment._candidate_allows_open(candidate))
        self.assertEqual(plan.leverage, 0)
        self.assertEqual(plan.stop_loss_distance_ratio, Decimal("0"))
        self.assertEqual(plan.required_margin_usdt, Decimal("0"))
        self.assertEqual(plan.planned_notional_usdt, Decimal("0"))

    def test_recent_records_can_be_filtered_by_created_at(self):
        fake_account = FakeAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            experiment.init_tables()
            with experiment._connect() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {experiment.TRADES_TABLE}
                    (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                     required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                     notional_usdt, take_profit_price, stop_loss_price, reason, created_at, updated_at)
                    VALUES ('OLD', 1, 'LONG', 'opened', 80, 4, '10', '2.5', '1000', '10', '1', '1', '10', '1.2', '0.9', 'old', 1000, 1000)
                    """
                )
                conn.execute(
                    f"""
                    INSERT INTO {experiment.TRADES_TABLE}
                    (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                     required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                     notional_usdt, take_profit_price, stop_loss_price, reason, created_at, updated_at)
                    VALUES ('NEW', 1, 'LONG', 'opened', 80, 5, '10', '2', '1000', '10', '1', '1', '10', '1.2', '0.9', 'new', 2000, 2000)
                    """
                )
                conn.execute(
                    f"""
                    INSERT INTO {experiment.ERRORS_TABLE}
                    (symbol, decision_round_ts, total_score, leverage, operation, error_type, error_message, created_at)
                    VALUES ('OLD', 1, 80, 4, 'open', 'RuntimeError', 'old', 1000)
                    """
                )
                conn.execute(
                    f"""
                    INSERT INTO {experiment.ERRORS_TABLE}
                    (symbol, decision_round_ts, total_score, leverage, operation, error_type, error_message, created_at)
                    VALUES ('NEW', 1, 80, 5, 'open', 'RuntimeError', 'new', 2000)
                    """
                )

            self.assertEqual([row.symbol for row in experiment.recent_trade_records(since_ms=1500)], ["NEW"])
            self.assertEqual([row.symbol for row in experiment.recent_error_records(since_ms=1500)], ["NEW"])

    def test_position_snapshot_uses_latest_opened_trade_leverage_when_position_risk_omits_it(self):
        class PositionWithoutLeverageAccountManager(FakeAccountManager):
            def _signed_get(self, endpoint, params=None):
                if endpoint == "/fapi/v3/positionRisk":
                    return [
                        {
                            "symbol": "BANKUSDT",
                            "positionAmt": "2",
                            "entryPrice": "1",
                            "markPrice": "1.1",
                            "unRealizedProfit": "0.2",
                            "notional": "2.2",
                            "liquidationPrice": "0.5",
                        }
                    ]
                return super()._signed_get(endpoint, params)

        fake_account = PositionWithoutLeverageAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            experiment.init_tables()
            with experiment._connect() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {experiment.TRADES_TABLE}
                    (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                     required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                     notional_usdt, take_profit_price, stop_loss_price, reason, created_at, updated_at)
                    VALUES ('BANKUSDT', 1, 'LONG', 'opened', 80, 6, '10', '1.66666667', '1000', '10', '1', '2', '2', '1.2', '0.9', 'new', 2000, 2000)
                    """
                )

            experiment._fetch_and_store_positions()
            snapshots = experiment.latest_position_snapshots()

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].symbol, "BANK")
        self.assertEqual(snapshots[0].leverage, "6")

    def test_position_snapshot_leverage_fallback_matches_base_trade_symbol_to_usdt_position(self):
        class PositionWithoutLeverageAccountManager(FakeAccountManager):
            def _signed_get(self, endpoint, params=None):
                if endpoint == "/fapi/v3/positionRisk":
                    return [
                        {
                            "symbol": "BANKUSDT",
                            "positionAmt": "2",
                            "entryPrice": "1",
                            "markPrice": "1.1",
                            "unRealizedProfit": "0.2",
                            "notional": "2.2",
                            "liquidationPrice": "0.5",
                        }
                    ]
                return super()._signed_get(endpoint, params)

        fake_account = PositionWithoutLeverageAccountManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            experiment = TradingExperiment(
                db_path=str(Path(tmpdir) / "klines.db"),
                account_manager=fake_account,
            )
            experiment.init_tables()
            with experiment._connect() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {experiment.TRADES_TABLE}
                    (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                     required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                     notional_usdt, take_profit_price, stop_loss_price, reason, created_at, updated_at)
                    VALUES ('BANK', 1, 'LONG', 'opened', 80, 7, '10', '1.42857143', '1000', '10', '1', '2', '2', '1.2', '0.9', 'new', 2000, 2000)
                    """
                )

            experiment._fetch_and_store_positions()
            snapshots = experiment.latest_position_snapshots()

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].symbol, "BANK")
        self.assertEqual(snapshots[0].leverage, "7")


if __name__ == "__main__":
    unittest.main()

class MissingExitOrdersAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.open_algo_orders = []

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            if params and params.get("symbol") == "BANKUSDT":
                return [{"symbol": "BANKUSDT", "positionAmt": "50", "entryPrice": "1"}]
            return [{"symbol": "BANKUSDT", "positionAmt": "50", "entryPrice": "1"}]
        if endpoint == "/fapi/v1/openAlgoOrders":
            return list(self.open_algo_orders)
        return super()._signed_get(endpoint, params)


def test_reconcile_missing_exit_orders_recreates_missing_take_profit_only(tmp_path):
    db_path = tmp_path / "klines.db"
    account = MissingExitOrdersAccountManager()
    account.open_algo_orders = [
        {"symbol": "BANKUSDT", "side": "SELL", "orderType": "STOP", "status": "NEW", "algoId": "sl-1"}
    ]
    experiment = TradingExperiment(
        db_path=str(db_path),
        account_manager=account,
        config=ExperimentConfig(exit_order_missing_position_retry_delay_seconds=Decimal("0")),
    )
    experiment.init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
             required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
             notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
             take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
            VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '1000', '10',
                    '1', '50', '50', '2.1', '0.8', '', '', 'sl-1', '', '', 1, 1)
            """
        )

    result = experiment.reconcile_missing_exit_orders(checked_at=2000)

    assert result["checked"] == 1
    assert result["created"] == 1
    assert result["errors"] == 0
    take_profit_posts = [params for endpoint, params in account.signed_posts if endpoint == "/fapi/v1/algoOrder" and params.get("type") == "TAKE_PROFIT"]
    stop_posts = [params for endpoint, params in account.signed_posts if endpoint == "/fapi/v1/algoOrder" and params.get("type") == "STOP"]
    assert len(take_profit_posts) == 1
    assert take_profit_posts[0]["symbol"] == "BANKUSDT"
    assert take_profit_posts[0]["quantity"] == "50"
    assert take_profit_posts[0]["triggerPrice"] == "2.1"
    assert stop_posts == []
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT take_profit_order_id, stop_loss_order_id FROM {TradingExperiment.TRADES_TABLE}").fetchone()
    assert row == ("1", "sl-1")


def test_reconcile_missing_exit_orders_cancels_stale_take_profit_without_position(tmp_path):
    db_path = tmp_path / "klines.db"
    account = MissingExitOrdersAccountManager()
    account.open_algo_orders = [
        {"symbol": "BANKUSDT", "side": "SELL", "orderType": "STOP", "status": "NEW", "algoId": "sl-1"},
        {"symbol": "GONEUSDT", "side": "SELL", "orderType": "TAKE_PROFIT", "status": "NEW", "algoId": "tp-stale"},
    ]
    experiment = TradingExperiment(
        db_path=str(db_path),
        account_manager=account,
        config=ExperimentConfig(exit_order_missing_position_retry_delay_seconds=Decimal("0")),
    )
    experiment.init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
             required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
             notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
             take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
            VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '1000', '10',
                    '1', '50', '50', '2.1', '0.8', '', 'tp-1', 'sl-1', '', '', 1, 1)
            """
        )

    result = experiment.reconcile_missing_exit_orders(checked_at=2000)

    assert result["checked"] == 1
    assert result["cancelled"] == 1
    assert ("/fapi/v1/algoOrder", {"symbol": "GONEUSDT", "algoId": "tp-stale"}) in account.signed_deletes
    assert ("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "sl-1"}) not in account.signed_deletes

class MaxStopOrderLimitAccountManager(FakeAccountManager):
    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": "BANKUSDT",
                        "filters": [
                            {"filterType": "LOT_SIZE", "stepSize": "1"},
                            {"filterType": "PRICE_FILTER", "tickSize": "0.000001", "minPrice": "0.000001", "maxPrice": "999999"},
                        ],
                    }
                ]
            }
        return super()._public_get(endpoint, params)

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/algoOrder":
            raise RuntimeError('400 Client Error: Bad Request; response_body={"code":-4045,"msg":"Reach max stop order limit."}')
        return {"orderId": len(self.signed_posts)}


def test_place_exit_order_falls_back_to_exchange_price_limit_when_stop_order_limit_reached(tmp_path):
    account = MaxStopOrderLimitAccountManager()
    experiment = TradingExperiment(db_path=str(tmp_path / "klines.db"), account_manager=account)
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

    response = experiment._place_exit_order(
        candidate,
        "BANKUSDT",
        {
            "symbol": "BANKUSDT",
            "side": "SELL",
            "type": "TAKE_PROFIT",
            "quantity": "50",
            "price": "2.1",
            "stopPrice": "2.1",
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "workingType": "MARK_PRICE",
        },
        "place_hard_take_profit",
    )

    assert response == {"orderId": 2}
    assert account.signed_posts[0][0] == "/fapi/v1/algoOrder"
    assert account.signed_posts[1] == (
        "/fapi/v1/order",
        {
            "symbol": "BANKUSDT",
            "side": "SELL",
            "type": "LIMIT",
            "quantity": "50",
            "price": "999999",
            "timeInForce": "GTC",
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        },
    )
