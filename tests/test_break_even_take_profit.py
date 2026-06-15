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
                "type": "STOP_MARKET",
                "closePosition": "true",
                "workingType": "MARK_PRICE",
                "triggerPrice": "10",
                "algoType": "CONDITIONAL",
            },
        )
    ]
    assert records[0].new_stop_loss_order_id == "456"
    assert records[0].stop_loss_price == "10"
