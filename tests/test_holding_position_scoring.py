import sqlite3
import tempfile
from pathlib import Path

from holding_position_scoring import HoldingPositionScoringSystem


class FakeAccountManager:
    def __init__(self):
        self.signed_posts = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "2"}]
        if endpoint == "/fapi/v1/userTrades":
            assert params == {"symbol": "BANKUSDT", "orderId": "123"}
            return [{"realizedPnl": "1.25"}, {"realizedPnl": "-0.5"}]
        raise AssertionError(f"unexpected endpoint {endpoint}")

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 123}


class RetryThenSuccessAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.user_trade_calls = 0

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/userTrades":
            self.user_trade_calls += 1
            if self.user_trade_calls == 1:
                return []
            if self.user_trade_calls == 2:
                raise RuntimeError("temporary lag")
            return [{"realizedPnl": "2.5"}]
        return super()._signed_get(endpoint, params)


class AlwaysMissingTradesAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.user_trade_calls = 0

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/userTrades":
            self.user_trade_calls += 1
            return []
        return super()._signed_get(endpoint, params)


def test_holding_position_scoring_strips_usdt_for_database_lookups_and_records():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
                [("BANK", 2000, 8), ("BANK", 1000, 9)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 2000, 10), ("BANK", 1000, 10)],
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=3000)
        round_ts, checks = scoring.get_latest_round_checks()
        records = scoring.recent_stop_loss_records()

    assert result["checked"] == 1
    assert result["triggered"] == 1
    assert round_ts == 3000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["reason"] == "two_15m_closes_below_structural_stop_loss"
    assert records[0]["symbol"] == "BANK"
    assert records[0]["realized_pnl"] == "0.75"
    assert fake_account.signed_posts[0][1]["symbol"] == "BANKUSDT"


def _seed_triggered_stop_loss_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
        conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
        conn.executemany(
            "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
            [("BANK", 2000, 8), ("BANK", 1000, 9)],
        )
        conn.executemany(
            "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
            [("BANK", 2000, 10), ("BANK", 1000, 10)],
        )


def test_realized_pnl_query_retries_until_trades_are_available():
    fake_account = RetryThenSuccessAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _seed_triggered_stop_loss_db(db_path)

        scoring = HoldingPositionScoringSystem(
            db_path=db_path,
            account_manager=fake_account,
            realized_pnl_retry_delays=(0, 0, 0),
        )
        scoring.run_round(decision_round_ts=3000)
        records = scoring.recent_stop_loss_records()

    assert fake_account.user_trade_calls == 3
    assert records[0]["realized_pnl"] == "2.5"
    assert "realized_pnl_query_failed" not in records[0]["reason"]


def test_realized_pnl_query_failure_is_written_to_reason():
    fake_account = AlwaysMissingTradesAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _seed_triggered_stop_loss_db(db_path)

        scoring = HoldingPositionScoringSystem(
            db_path=db_path,
            account_manager=fake_account,
            realized_pnl_retry_delays=(0, 0, 0),
        )
        scoring.run_round(decision_round_ts=3000)
        records = scoring.recent_stop_loss_records()

    assert fake_account.user_trade_calls == 4
    assert records[0]["realized_pnl"] == ""
    assert "realized_pnl_query_failed_after_4_attempts: user_trades_empty" in records[0]["reason"]
