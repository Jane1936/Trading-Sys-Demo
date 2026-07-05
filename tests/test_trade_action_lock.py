import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from partial_take_profit import PartialTakeProfitStrategy
from trade_action_lock import TradeActionLockManager


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
            return [{"symbol": "BANKUSDT", "positionAmt": "10", "entryPrice": "10", "markPrice": "12.5", "unRealizedProfit": self.unrealized_profit, "leverage": "5", "notional": "125", "liquidationPrice": "0"}]
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        raise AssertionError(f"unexpected public endpoint {endpoint}")

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"status": "CANCELED"}

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 789}


def _insert_open_trade(db_path):
    import sqlite3
    from trading_experiment import TradingExperiment

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


def test_trade_action_lock_allows_only_one_active_owner_per_symbol():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        manager = TradeActionLockManager(db_path=db_path, ttl_ms=60_000)

        first = manager.acquire("BANK", "owner-a", "close", now_ms=1_000)
        second = manager.acquire("BANK", "owner-b", "increase", now_ms=1_001)

        assert first is not None
        assert second is None
        assert "locked_by=owner-a" in manager.busy_reason("BANK", "owner-b", "increase")

        manager.release(first)
        third = manager.acquire("BANK", "owner-b", "increase", now_ms=1_002)
        assert third is not None


def test_partial_take_profit_records_lock_error_without_ordering():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_open_trade(db_path)
        lock = TradeActionLockManager(db_path=db_path).acquire("BANK", "other-module", "force_close")
        assert lock is not None
        strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=fake_account)

        result = strategy.run_round()
        records = strategy.recent_records()

    assert result["checked"] == 1
    assert result["triggered"] == 1
    assert result["records"] == 1
    assert fake_account.signed_posts == []
    assert records[0].status == "failed"
    assert "trade_action_lock_busy" in records[0].reason
    assert "locked_by=other-module" in records[0].reason
