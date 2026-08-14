import sqlite3
import tempfile
from decimal import Decimal
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


class FailingOnceAccountManager(FakeAccountManager):
    def __init__(self, unrealized_profit="30"):
        super().__init__(unrealized_profit)
        self.fail_next_post = True

    def _signed_post(self, endpoint, params=None):
        if self.fail_next_post:
            self.fail_next_post = False
            raise RuntimeError("temporary order failure")
        return super()._signed_post(endpoint, params)


def _seed_db(db_path, close, high=20):
    TradingExperiment(db_path=db_path, account_manager=FakeAccountManager()).init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE klines_1m (symbol TEXT, open_time INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL, close_time INTEGER, PRIMARY KEY(symbol, open_time))")
        conn.execute("INSERT INTO klines_1m VALUES ('BANK', 1000, 10, ?, 9, ?, 100, 59999)", (high, close))
        conn.execute(f"INSERT INTO {TradingExperiment.TRADES_TABLE} (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '5000', '50', '10', '10', '100', '18', '8', '', 'tp-1', 'sl-1', '', '', 1, 1)")


def test_dynamic_profit_protection_closes_when_2r_to_3r_profit_draws_down_40_percent():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=11.8, high=13)
        account = FakeAccountManager(unrealized_profit="30")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["triggered"] == 1
    assert checks[0].triggered is True
    assert checks[0].drawdown_threshold == "0.4"
    assert checks[0].current_tier == "(2R, 3R]"
    assert account.signed_posts[-1][1]["type"] == "MARKET"
    assert account.signed_posts[-1][1]["quantity"] == "10"


def test_dynamic_profit_protection_updates_failed_action_record_on_retry():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=11.8, high=13)
        account = FailingOnceAccountManager()
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)

        assert tracker.run_round()["triggered"] == 0
        failed = tracker.recent_action_records()
        assert len(failed) == 1
        assert failed[0].close_status == "failed"

        assert tracker.run_round()["triggered"] == 1
        records = tracker.recent_action_records()

    assert len(records) == 1
    assert records[0].close_status == "submitted"
    assert records[0].close_order_id == "789"


def test_dynamic_profit_protection_does_not_close_below_2r():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=10.5, high=11.9)
        account = FakeAccountManager(unrealized_profit="19")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["triggered"] == 0
    assert checks[0].eligible is False
    assert account.signed_posts == []


def test_dynamic_profit_protection_uses_highest_reached_tier_priority_over_current_profit():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=22, high=50)
        account = FakeAccountManager(unrealized_profit="60")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["triggered"] == 1
    assert checks[0].current_tier == "4R以上"
    assert checks[0].highest_unrealized_pnl == "400"
    assert checks[0].drawdown_threshold == "0.2"
    assert "tier=4R以上" in checks[0].reason
    assert account.signed_posts[-1][1]["type"] == "MARKET"


def test_dynamic_profit_protection_tier_boundaries():
    tier_for = DynamicProfitProtection._tier_and_threshold_for_reached_r_multiple

    assert tier_for(Decimal("2")) == ("未达档", Decimal("0"))
    assert tier_for(Decimal("2.0001")) == ("(2R, 3R]", Decimal("0.40"))
    assert tier_for(Decimal("3")) == ("(2R, 3R]", Decimal("0.40"))
    assert tier_for(Decimal("3.0001")) == ("(3R, 4R]", Decimal("0.30"))
    assert tier_for(Decimal("4")) == ("(3R, 4R]", Decimal("0.30"))
    assert tier_for(Decimal("4.0001")) == ("4R以上", Decimal("0.20"))


def test_dynamic_profit_protection_resets_highest_for_latest_open_trade_even_if_old_trade_still_opened():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=49, high=50)
        account = FakeAccountManager(unrealized_profit="20")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=account)
        tracker.run_round()

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"INSERT INTO {TradingExperiment.TRADES_TABLE} "
                "(symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) "
                "VALUES ('BANK', 2, 'LONG', 'opened', 80, 5, '100', '20', '5000', '50', '10', '10', '100', '18', '8', '', 'tp-2', 'sl-2', '', '', 2000, 2000)"
            )
            conn.execute("INSERT INTO klines_1m VALUES ('BANK', 2000, 10, 13, 9, 12, 100, 61999)")

        tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert checks[0].highest_since_open == "13"
    assert checks[0].highest_profit_at == 2000
    assert checks[0].opened_at == 2000
    assert checks[0].open_trade_id > 0


def test_dynamic_profit_protection_rejects_high_candidates_before_latest_open():
    highest, highest_at = DynamicProfitProtection._newer_open_highest(
        2_000,
        (Decimal("50"), 1_000),
        (Decimal("13"), 2_000),
    )

    assert highest == Decimal("13")
    assert highest_at == 2_000


def test_dynamic_profit_protection_records_first_time_of_equal_post_open_high():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        _seed_db(db_path, close=12, high=13)
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO klines_1m VALUES ('BANK', 2000, 12, 13, 11, 12, 100, 61999)")

        tracker = DynamicProfitProtection(db_path=db_path, account_manager=FakeAccountManager(unrealized_profit="20"))
        tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert checks[0].highest_since_open == "13"
    assert checks[0].highest_profit_at == 1000


def test_dynamic_profit_protection_migrates_legacy_high_timestamp_name():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "k.db")
        tracker = DynamicProfitProtection(db_path=db_path, account_manager=FakeAccountManager())
        tracker.init_tables()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"ALTER TABLE {tracker.CHECKS_TABLE} "
                "ADD COLUMN highest_since_open_at INTEGER NOT NULL DEFAULT 0"
            )
            conn.execute(
                f"INSERT INTO {tracker.CHECKS_TABLE} "
                "(symbol, checked_at, opened_at, entry_price, position_amt, highest_since_open, "
                "highest_profit_at, highest_since_open_at, reason) "
                "VALUES ('BANK', 3000, 1000, '10', '2', '13', 0, 2000, 'legacy')"
            )

        tracker.init_tables()
        _, checks = tracker.get_latest_round_checks()

    assert checks[0].highest_profit_at == 2000


def test_latest_open_trade_falls_back_to_rowid_when_legacy_id_is_null():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "legacy.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"CREATE TABLE {TradingExperiment.TRADES_TABLE} "
                "(id INT, symbol TEXT, status TEXT, created_at INTEGER)"
            )
            cursor = conn.execute(
                f"INSERT INTO {TradingExperiment.TRADES_TABLE} "
                "(id, symbol, status, created_at) VALUES (NULL, 'BANK', 'opened', 123456)"
            )
            rowid = cursor.lastrowid

        tracker = DynamicProfitProtection(db_path=db_path)

        assert tracker._latest_open_trade("BANK") == (rowid, 123456)
