import sqlite3
import tempfile
import unittest
from pathlib import Path

from break_even_take_profit import BreakEvenTakeProfitStrategy
from trading_experiment import TradingExperiment
from zombie_force_liquidation import ZombieForceLiquidationModule


class ZombieAccountManager:
    def __init__(self):
        self.deleted = []
        self.posts = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "2", "entryPrice": "10", "markPrice": "9", "unRealizedProfit": "-2", "leverage": "5", "notional": "18", "liquidationPrice": "0"}]
        raise AssertionError(f"unexpected signed get {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {"symbols": [{"symbol": "BANKUSDT", "filters": [{"filterType": "LOT_SIZE", "stepSize": "0.1"}, {"filterType": "PRICE_FILTER", "tickSize": "0.01"}]}]}
        if endpoint == "/fapi/v1/ticker/price":
            return {"price": "9"}
        raise AssertionError(f"unexpected public get {endpoint}")

    def _signed_delete(self, endpoint, params=None):
        self.deleted.append((endpoint, dict(params or {})))
        return {"ok": True, "endpoint": endpoint}

    def _signed_post(self, endpoint, params=None):
        self.posts.append((endpoint, dict(params or {})))
        return {"orderId": 123, "executedQty": params.get("quantity", "0"), "cumQuote": "18"}


class ZombieForceLiquidationTests(unittest.TestCase):
    def test_closes_position_older_than_48h_without_break_even_record(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            account = ZombieAccountManager()
            TradingExperiment(db_path=db_path, account_manager=account).init_tables()
            opened_at = 1_000
            checked_at = opened_at + 49 * 60 * 60 * 1000
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    f"""
                    INSERT INTO {TradingExperiment.TRADES_TABLE}
                    (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt,
                     required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity,
                     notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation,
                     take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
                    VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '1000', '10', '10', '2',
                            '20', '37.5', '7', '', 'tp-1', 'sl-1', '', '', ?, ?)
                    """,
                    (opened_at, opened_at),
                )

            result = ZombieForceLiquidationModule(db_path=db_path, account_manager=account).run_round(checked_at=checked_at)

            self.assertEqual(result["checked"], 1)
            self.assertEqual(result["triggered"], 1)
            self.assertEqual(account.deleted[0], ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}))
            self.assertEqual(account.deleted[1], ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}))
            self.assertEqual(account.posts[-1][0], "/fapi/v1/order")
            self.assertEqual(account.posts[-1][1]["side"], "SELL")
            self.assertEqual(account.posts[-1][1]["type"], "MARKET")
            self.assertEqual(account.posts[-1][1]["quantity"], "2")
            self.assertEqual(account.posts[-1][1]["reduceOnly"], "true")

    def test_skips_old_position_with_lifecycle_break_even_record(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "klines.db")
            account = ZombieAccountManager()
            TradingExperiment(db_path=db_path, account_manager=account).init_tables()
            BreakEvenTakeProfitStrategy(db_path=db_path, account_manager=account).init_tables()
            opened_at = 1_000
            checked_at = opened_at + 49 * 60 * 60 * 1000
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    f"INSERT INTO {TradingExperiment.TRADES_TABLE} (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '1000', '10', '10', '2', '20', '37.5', '7', '', 'tp-1', 'sl-1', '', '', ?, ?)",
                    (opened_at, opened_at),
                )
                conn.execute(
                    f"INSERT INTO {BreakEvenTakeProfitStrategy.RECORDS_TABLE} (symbol, checked_at, side, position_amt, entry_price, account_equity_usdt, r_usdt, unrealized_pnl, old_stop_loss_order_id, new_stop_loss_order_id, stop_loss_price, status, reason, raw_response) VALUES ('BANK', ?, 'SELL', '2', '10', '1000', '10', '12', 'sl-1', 'sl-2', '10', 'submitted', '', '')",
                    (opened_at + 1,),
                )

            result = ZombieForceLiquidationModule(db_path=db_path, account_manager=account).run_round(checked_at=checked_at)

            self.assertEqual(result["checked"], 1)
            self.assertEqual(result["triggered"], 0)
            self.assertEqual(account.deleted, [])
            self.assertEqual(account.posts, [])


if __name__ == "__main__":
    unittest.main()
