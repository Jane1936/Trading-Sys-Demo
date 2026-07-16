import sqlite3
import tempfile
from pathlib import Path

from dynamic_profit_protection import DynamicProfitProtection
from trading_experiment import TradingExperiment


class FakeAccountManager:
    def __init__(self, unrealized_profit="50"):
        self.unrealized_profit = unrealized_profit
        self.signed_deletes = []
        self.signed_posts = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5000"}]
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "10", "entryPrice": "10", "markPrice": "15", "unRealizedProfit": self.unrealized_profit, "leverage": "5", "notional": "150"}]
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/exchangeInfo":
            return {"symbols": [{"symbol": "BANKUSDT", "filters": [{"filterType": "LOT_SIZE", "stepSize": "0.1"}, {"filterType": "PRICE_FILTER", "tickSize": "0.01"}]}]}
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"status": "CANCELED"}

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 789}


def _seed_db(db_path, close):
    TradingExperiment(db_path=db_path, account_manager=FakeAccountManager()).init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE klines_1m (symbol TEXT, open_time INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL, close_time INTEGER, PRIMARY KEY(symbol, open_time))")
        conn.execute("INSERT INTO klines_1m VALUES ('BANK', 1000, 10, 20, 9, ?, 100, 59999)", (close,))
        conn.execute(f"INSERT INTO {TradingExperiment.TRADES_TABLE} (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '5000', '50', '10', '10', '100', '18', '8', '', 'tp-1', 'sl-1', '', '', 1, 1)")


def test_dynamic_profit_protection_closes_when_2r_to_4r_profit_draws_down_40_percent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=16)
        account = FakeAccountManager(unrealized_profit="30")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["triggered"] == 1
    assert checks[0].triggered is True
    assert checks[0].drawdown_threshold == "0.4"
    assert account.signed_posts[-1][1]["type"] == "MARKET"
    assert account.signed_posts[-1][1]["quantity"] == "10"


def test_dynamic_profit_protection_does_not_close_below_2r():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=16)
        account = FakeAccountManager(unrealized_profit="19")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["triggered"] == 0
    assert checks[0].eligible is False
    assert account.signed_posts == []
