import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path

from holding_position_scoring import HoldingPositionScoringSystem
from trading_experiment import TradingExperiment

class FakeAccountManager:
    def __init__(self):
        self.signed_posts = []
        self.signed_deletes = []

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "2", "leverage": "5"}]
        if endpoint == "/fapi/v1/userTrades":
            assert params == {"symbol": "BANKUSDT", "orderId": "123"}
            return [{"realizedPnl": "1.25"}, {"realizedPnl": "-0.5"}]
        if endpoint == "/fapi/v3/balance":
            return [{"asset": "USDT", "balance": "5000"}]
        raise AssertionError(f"unexpected endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/price":
            return {"price": "8"}
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
        return {"orderId": 123}

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"code": 200, "msg": "success"}

class ElevenPositionsAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": f"COIN{i}USDT", "positionAmt": "1", "leverage": "1"} for i in range(11)]
        return super()._signed_get(endpoint, params)

class HighPortfolioRiskAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {"symbol": "BANKUSDT", "positionAmt": "200", "leverage": "10"},
                {"symbol": "COINUSDT", "positionAmt": "200", "leverage": "10"},
            ]
        if endpoint == "/fapi/v1/userTrades":
            return []
        return super()._signed_get(endpoint, params)

class FailingPortfolioRiskAccountManager(HighPortfolioRiskAccountManager):
    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        raise RuntimeError("market close rejected")

class TiedLowestScorePortfolioRiskAccountManager(FakeAccountManager):
    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [
                {"symbol": "BANKUSDT", "positionAmt": "200", "leverage": "10", "unRealizedProfit": "5"},
                {"symbol": "COINUSDT", "positionAmt": "200", "leverage": "10", "unRealizedProfit": "-3"},
                {"symbol": "FUNDUSDT", "positionAmt": "200", "leverage": "10", "unRealizedProfit": "-1.5"},
            ]
        if endpoint == "/fapi/v1/userTrades":
            return []
        return super()._signed_get(endpoint, params)

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

class ExpiredNoFillAccountManager(FakeAccountManager):
    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 321, "status": "EXPIRED", "executedQty": "0"}

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/userTrades":
            raise AssertionError("should not query realized PnL for an unfilled stop-loss order")
        return super()._signed_get(endpoint, params)

class ReduceOnlyRejectedAccountManager(FakeAccountManager):
    def __init__(self):
        super().__init__()
        self.diagnostic_gets = []

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        raise RuntimeError('400 Client Error; response_body={"code":-2022,"msg":"ReduceOnly Order is rejected."}')

    def _signed_get(self, endpoint, params=None):
        self.diagnostic_gets.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "2", "positionSide": "BOTH"}]
        if endpoint == "/fapi/v1/openOrders":
            return [{"symbol": "BANKUSDT", "side": "SELL", "type": "STOP_MARKET", "origQty": "2", "reduceOnly": True, "status": "NEW", "orderId": 456}]
        if endpoint == "/fapi/v1/openAlgoOrders":
            return [{"symbol": "BANKUSDT", "side": "SELL", "type": "TAKE_PROFIT_MARKET", "closePosition": True, "status": "NEW", "algoId": 789}]
        return super()._signed_get(endpoint, params)

class StopLossBeforeReductionScoring(HoldingPositionScoringSystem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reduction_started_after_stop_loss_check = False

    def evaluate_reduction_conditions(self, positions=None, decision_round_ts=None, checked_at=None):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {self.CHECKS_TABLE} WHERE decision_round_ts = ?",
                (decision_round_ts,),
            ).fetchone()
        self.reduction_started_after_stop_loss_check = bool(row and row[0] == 1)
        return []

class StopLossRecordBeforeReductionScoring(HoldingPositionScoringSystem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reduction_started_after_stop_loss_record = False

    def evaluate_reduction_conditions(self, positions=None, decision_round_ts=None, checked_at=None):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {self.RECORDS_TABLE} WHERE decision_round_ts = ?",
                (decision_round_ts,),
            ).fetchone()
        self.reduction_started_after_stop_loss_record = bool(row and row[0] == 1)
        return []

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
    assert fake_account.signed_deletes == [
        ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}),
    ]
    assert fake_account.signed_posts[0][1]["symbol"] == "BANKUSDT"

def test_position_reduction_runs_after_stop_loss_judgement_is_persisted():
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
                [("BANK", 2000, 7), ("BANK", 1000, 7)],
            )

        scoring = StopLossBeforeReductionScoring(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=3000)

    assert result["checked"] == 1
    assert result["reduction_checked"] == 0
    assert scoring.reduction_started_after_stop_loss_check is True

def test_position_reduction_runs_after_triggered_stop_loss_record_is_persisted():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
                [("BANK", 2000, 8), ("BANK", 1000, 9)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 2000, 10), ("BANK", 1000, 10)],
            )
            conn.execute("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", ("BANK", 3000, 30))

        scoring = StopLossRecordBeforeReductionScoring(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=3000)

    assert result["records"] == 1
    assert result["reduction_checked"] == 0
    assert scoring.reduction_started_after_stop_loss_record is True

def test_position_reduction_waits_for_current_round_total_scores():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, high, close) VALUES (?, ?, ?, ?)",
                [("BANK", 3000, 10, 8), ("BANK", 2000, 9, 8), ("BANK", 1000, 8, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", [("BANK", 3000, 20), ("BANK", 2000, 50)])

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)

    assert result["reduction_checked"] == 0
    assert result["reduction_triggered"] == 0

def test_portfolio_risk_runs_after_holding_stop_loss_round():
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
                [("BANK", 2000, 7), ("BANK", 1000, 7)],
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=3000)
        risk = scoring.get_latest_portfolio_risk()

    assert result["total_risk"] == "0.08"
    assert result["risk_position_count"] == 1
    assert risk is not None
    assert risk.total_risk == "0.08"
    assert risk.positions[0].symbol == "BANK"
    assert risk.positions[0].position_amt == "2"
    assert risk.positions[0].latest_15m_close == "8"
    assert risk.positions[0].account_equity_usdt == "1000"
    assert risk.positions[0].leverage == "5"
    assert risk.positions[0].risk == "0.08"

def test_portfolio_risk_includes_all_positions_without_ten_position_cap():
    fake_account = ElevenPositionsAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            for i in range(11):
                conn.execute("INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)", (f"COIN{i}", 1, 1))
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)

        risk = scoring.calculate_portfolio_risk(decision_round_ts=1)

    assert risk.position_count == 11
    assert len(risk.positions) == 11

def test_portfolio_risk_displays_scores_without_forced_liquidation_when_total_risk_gt_18():
    fake_account = HighPortfolioRiskAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
                [("BANK", 2000, 5), ("COIN", 2000, 5)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 2000, 1), ("COIN", 2000, 1)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 2000, 80), ("COIN", 2000, 60)],
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        result = scoring.run_round(decision_round_ts=3000)
        risk = scoring.get_latest_portfolio_risk()
        stop_loss_records = scoring.recent_stop_loss_records()

    assert result["total_risk"] == "20"
    assert result["portfolio_risk_action"] == ""
    assert risk is not None
    assert {row.symbol: row.total_score for row in risk.positions} == {"BANK": "80", "COIN": "60"}
    assert fake_account.signed_posts == []
    assert stop_loss_records == []

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

def test_unfilled_stop_loss_order_response_is_recorded_as_failed_without_pnl_lookup():
    fake_account = ExpiredNoFillAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _seed_triggered_stop_loss_db(db_path)

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        scoring.run_round(decision_round_ts=3000)
        records = scoring.recent_stop_loss_records()

    assert records[0]["status"] == "failed"
    assert records[0]["order_id"] == ""
    assert records[0]["realized_pnl"] == ""
    assert "stop_loss_order_not_filled: status=EXPIRED; executedQty=0" in records[0]["reason"]
    assert "realized_pnl_query_failed" not in records[0]["reason"]

def test_reduce_only_rejection_records_positions_and_open_order_diagnostics():
    fake_account = ReduceOnlyRejectedAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _seed_triggered_stop_loss_db(db_path)

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        scoring.run_round(decision_round_ts=3000)
        records = scoring.recent_stop_loss_records()

    assert fake_account.signed_deletes == [
        ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}),
    ]
    assert ("/fapi/v3/positionRisk", {}) in fake_account.diagnostic_gets
    assert ("/fapi/v1/openOrders", {"symbol": "BANKUSDT"}) in fake_account.diagnostic_gets
    assert ("/fapi/v1/openAlgoOrders", {"symbol": "BANKUSDT"}) in fake_account.diagnostic_gets
    assert records[0]["status"] == "failed"
    assert "stop_loss_order_failed" in records[0]["reason"]
    assert "reduce_only_diagnostics:" in records[0]["reason"]
    assert "positions=[{symbol=BANKUSDT, positionAmt=2, positionSide=BOTH}] total=1" in records[0]["reason"]
    assert "open_orders=[{symbol=BANKUSDT, side=SELL, type=STOP_MARKET" in records[0]["reason"]
    assert "open_algo_orders=[{symbol=BANKUSDT, side=SELL, type=TAKE_PROFIT_MARKET" in records[0]["reason"]

def test_position_reduction_rule_tags_absolute_score_large_drawdown():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, high, close) VALUES (?, ?, ?, ?)",
                [("BANK", 3000, 10, 8), ("BANK", 2000, 9, 8), ("BANK", 1000, 8, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", [("BANK", 4000, 52), ("BANK", 3000, 70), ("BANK", 2000, 80)])
            conn.execute("INSERT INTO trading_experiment_trades (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)", ("BANK", "opened", 80, 1000))

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 1
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 1
    assert checks[0]["tag"] == "绝对分数大幅回撤"
    assert checks[0]["reason"] == "absolute_score_large_drawdown"
    assert checks[0]["rule_name"] == "规则三"
    assert checks[0]["highest_15m_high"] == "10"
    assert checks[0]["current_price"] == "8"
    assert checks[0]["two_r_usdt"] == "20"
    assert checks[0]["unrealized_pnl"] == "0"
    assert checks[0]["open_total_score"] == "80"
    assert checks[0]["latest_total_score"] == "52"
    assert checks[0]["score_drawdown"] == "28"
    assert checks[0]["recent_score_drawdown"] == "18"

def test_position_reduction_rule_tags_medium_drawdown_and_continuous_weakening():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 10, 8, 8), ("BANK", 2000, 9, 8, 8), ("BANK", 1000, 8, 8, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 53), ("BANK", 3000, 70), ("BANK", 2000, 80)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
                ("BANK", "opened", 80, 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 1
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 1
    assert checks[0]["tag"] == "中等回撤且趋势连续弱化"
    assert checks[0]["reason"] == "medium_drawdown_and_continuous_weakening"
    assert checks[0]["latest_15m_open"] == "10"
    assert checks[0]["latest_15m_close"] == "8"
    assert checks[0]["open_total_score"] == "80"
    assert checks[0]["latest_total_score"] == "53"
    assert checks[0]["previous_total_score"] == "70"
    assert checks[0]["score_drawdown"] == "27"
    assert checks[0]["recent_score_drawdown"] == "17"
    assert checks[0]["rule_name"] == "规则二"

def test_position_reduction_no_longer_triggers_on_removed_rule_four_score_danger_zone():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 8, 8.1, 8), ("BANK", 2000, 8, 8.1, 8), ("BANK", 1000, 8, 8.1, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.execute("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", ("BANK", 4000, 39))
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
                ("BANK", "opened", 45, 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 0
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 0
    assert checks[0]["tag"] == ""
    assert checks[0]["rule_name"] == ""
    assert checks[0]["latest_total_score"] == "39"

def test_position_reduction_rule_tags_medium_danger_zone_price_confirmation():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    entry_price TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 8, 8.1, 8), ("BANK", 2000, 8, 8.1, 8), ("BANK", 1000, 8, 8.1, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", [("BANK", 4000, 30), ("BANK", 3000, 25)])
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, entry_price, created_at) VALUES (?, ?, ?, ?, ?)",
                ("BANK", "opened", 35, "8.5", 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 1
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 1
    assert checks[0]["tag"] == "中危险区+价格确认"
    assert checks[0]["reason"] == "medium_danger_zone_price_confirmation"
    assert checks[0]["rule_name"] == "规则五"
    assert checks[0]["latest_total_score"] == "30"
    assert checks[0]["current_price"] == "8"
    assert checks[0]["open_entry_price"] == "8.5"

def test_position_reduction_rule5_condition2_requires_recent_two_closes_below_entry():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    entry_price TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 8, 8.1, 8), ("BANK", 2000, 8, 8.8, 8.7), ("BANK", 1000, 8, 8.1, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 30), ("BANK", 3000, 25)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, entry_price, created_at) VALUES (?, ?, ?, ?, ?)",
                ("BANK", "opened", 35, "8.5", 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 0
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 0
    assert checks[0]["tag"] == ""
    assert checks[0]["rule_name"] == ""
    assert checks[0]["reason"].endswith("rule5_condition2_recent_two_15m_closes_not_all_lte_open_entry_price")

def test_position_reduction_rule5_only_triggers_once_per_open_lifecycle():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("""
                CREATE TABLE trading_experiment_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total_score INTEGER,
                    entry_price TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
            """)
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 8, 8.1, 8), ("BANK", 2000, 8, 8.1, 8), ("BANK", 1000, 8, 8.1, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 18), ("BANK", 3000, 35)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, entry_price, created_at) VALUES (?, ?, ?, ?, ?)",
                ("BANK", "opened", 35, "8.5", 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        scoring.init_tables()
        TradingExperiment(db_path=db_path, account_manager=fake_account).init_tables()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"INSERT INTO {scoring.REDUCTION_RECORDS_TABLE} (symbol, decision_round_ts, side, matched_rule, reduction_percent, original_quantity, reduced_quantity, remaining_quantity, status, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("BANK", 3500, "SELL", "规则五", "0.5", "2", "1", "1", "success", "first rule5", 2000),
            )

        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 0
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 0
    assert checks[0]["tag"] == ""
    assert checks[0]["rule_name"] == ""
    assert checks[0]["reason"].endswith("rule5_already_triggered_in_current_open_lifecycle")

def test_reduction_action_uses_highest_rule_replaces_limit_and_market_reduces():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute(
                "CREATE TABLE trading_experiment_trades (symbol TEXT, status TEXT, total_score INTEGER, entry_price TEXT, stop_loss_price TEXT, stop_loss_order_id TEXT, created_at INTEGER, id INTEGER)"
            )
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)",
                [("BANK", 3000, 10, 10, 8), ("BANK", 2000, 10, 11, 10), ("BANK", 1000, 10, 10, 10)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 1), ("BANK", 2000, 1)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 18), ("BANK", 3000, 25)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, entry_price, stop_loss_price, stop_loss_order_id, created_at, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("BANK", "opened", 80, "9", "7", "old-sl-1", 1, 1),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        result = scoring.run_round(decision_round_ts=4000)
        records = scoring.recent_reduction_records()

    assert result["reduction_records"] == 1
    assert records[0]["matched_rule"] == "规则五"
    assert records[0]["reduction_percent"] == "0.5"
    assert records[0]["reduced_quantity"] == "1"
    assert records[0]["remaining_quantity"] == "1"
    assert records[0]["old_limit_order_id"] == "old-sl-1"
    assert records[0]["new_limit_order_id"] == "123"
    assert records[0]["market_order_id"] == "123"
    assert ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}) in fake_account.signed_deletes
    assert fake_account.signed_posts[-1] == (
        "/fapi/v1/order",
        {"symbol": "BANKUSDT", "side": "SELL", "type": "MARKET", "quantity": "1", "reduceOnly": "true", "newOrderRespType": "RESULT"},
    )

def test_reduction_action_skips_replacement_stop_that_would_immediately_trigger_and_still_reduces():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute(
                "CREATE TABLE trading_experiment_trades (symbol TEXT, status TEXT, total_score INTEGER, stop_loss_price TEXT, stop_loss_order_id TEXT, created_at INTEGER, id INTEGER)"
            )
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, high, close) VALUES (?, ?, ?, ?)",
                [("BANK", 3000, 10, 8), ("BANK", 2000, 9, 8), ("BANK", 1000, 8, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 62), ("BANK", 3000, 80), ("BANK", 2000, 81)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, stop_loss_price, stop_loss_order_id, created_at, id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("BANK", "opened", 80, "9", "old-sl-1", 1, 1),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        result = scoring.run_round(decision_round_ts=4000)
        records = scoring.recent_reduction_records()

    assert result["reduction_records"] == 1
    assert records[0]["matched_rule"] == "规则三"
    assert records[0]["reduction_percent"] == "0.3"
    assert records[0]["reduced_quantity"] == "0.6"
    assert records[0]["remaining_quantity"] == "1.4"
    assert records[0]["new_limit_order_id"] == ""
    assert records[0]["market_order_id"] == "123"
    assert "reduction_replacement_stop_skipped_immediate_trigger" in records[0]["reason"]
    assert fake_account.signed_posts == [
        (
            "/fapi/v1/order",
            {"symbol": "BANKUSDT", "side": "SELL", "type": "MARKET", "quantity": "0.6", "reduceOnly": "true", "newOrderRespType": "RESULT"},
        )
    ]

def test_position_increase_triggers_first_add_once_per_open_lifecycle():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("CREATE TABLE trading_experiment_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, status TEXT, created_at INTEGER, total_score TEXT, entry_price TEXT, stop_loss_order_id TEXT, stop_loss_price TEXT)")
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 3000, 70), ("BANK", 2000, 73)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, created_at, total_score, entry_price, stop_loss_order_id, stop_loss_price) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("BANK", "opened", 1000, "72", "7", "", ""),
            )
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        positions = [{"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": "8", "unRealizedProfit": "70"}]

        checks = scoring.evaluate_increase_conditions(positions=positions, decision_round_ts=3000, checked_at=4000)
        records = scoring._record_increase_actions(checks, now_ms=4000)
        second_checks = scoring.evaluate_increase_conditions(positions=positions, decision_round_ts=3000, checked_at=5000)

    assert checks[0].triggered is True
    assert checks[0].tag == "第一次加仓"
    assert records == 1
    assert second_checks[0].triggered is False
    assert second_checks[0].reason == "first_increase_already_triggered_in_current_lifecycle"


def test_position_increase_requires_current_price_not_below_lifecycle_reduction_price():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.execute("CREATE TABLE trading_experiment_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, status TEXT, created_at INTEGER, total_score TEXT, entry_price TEXT, stop_loss_order_id TEXT, stop_loss_price TEXT)")
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 3000, 75), ("BANK", 2000, 76)],
            )
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, created_at, total_score, entry_price, stop_loss_order_id, stop_loss_price) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("BANK", "opened", 1000, "72", "7", "", ""),
            )
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        scoring.init_tables()
        TradingExperiment(db_path=db_path, account_manager=fake_account).init_tables()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"INSERT INTO {scoring.REDUCTION_RECORDS_TABLE} (symbol, decision_round_ts, side, matched_rule, reduction_percent, original_quantity, reduced_quantity, remaining_quantity, status, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("BANK", 2500, "SELL", "规则三", "0.3", "2", "0.6", "1.4", "submitted", "test", 2000),
            )
            conn.execute(
                f"INSERT INTO {scoring.REDUCTION_CHECKS_TABLE} (symbol, decision_round_ts, highest_15m_high, current_price, price_drawdown_ratio, account_equity_usdt, two_r_usdt, one_r_usdt, unrealized_pnl, open_total_score, latest_total_score, score_drawdown, triggered, tag, reason, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("BANK", 2500, "10", "9", "0.1", "5000", "100", "50", "20", "72", "70", "2", 1, "tag", "test", 2000),
            )
        positions = [{"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": "8", "unRealizedProfit": "70"}]

        checks = scoring.evaluate_increase_conditions(positions=positions, decision_round_ts=3000, checked_at=4000)

    assert checks[0].triggered is False
    assert checks[0].latest_reduction_price == "9"
    assert checks[0].reason == "condition3_current_price_lt_latest_reduction_price"

class PositionChangeHardTakeProfitAccountManager(FakeAccountManager):
    def __init__(self, refreshed_amount="1.5", refreshed_entry="10"):
        super().__init__()
        self.refreshed_amount = refreshed_amount
        self.refreshed_entry = refreshed_entry

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk" and params and params.get("symbol") == "BANKUSDT":
            return [{"symbol": "BANKUSDT", "positionAmt": self.refreshed_amount, "entryPrice": self.refreshed_entry, "leverage": "5"}]
        if endpoint == "/fapi/v3/account":
            return {"availableBalance": "5000"}
        return super()._signed_get(endpoint, params)

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        if endpoint == "/fapi/v1/algoOrder":
            return {"algoId": 900 + len(self.signed_posts)}
        return {"orderId": 100 + len(self.signed_posts)}


def test_reduction_recreates_hard_take_profit_from_actual_remaining_position():
    fake_account = PositionChangeHardTakeProfitAccountManager(refreshed_amount="1.5", refreshed_entry="10")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        scoring.init_tables()
        TradingExperiment(db_path=db_path, account_manager=fake_account).init_tables()
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, open REAL, high REAL, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany("INSERT INTO klines_15m (symbol, open_time, open, high, close) VALUES (?, ?, ?, ?, ?)", [("BANK", 3000, 10, 10, 8), ("BANK", 2000, 10, 11, 10), ("BANK", 1000, 10, 10, 10)])
            conn.executemany("INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)", [("BANK", 3000, 1), ("BANK", 2000, 1)])
            conn.executemany("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", [("BANK", 4000, 18), ("BANK", 3000, 25)])
            conn.execute(f"INSERT INTO {TradingExperiment.TRADES_TABLE} (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) VALUES ('BANK', 1, 'LONG', 'opened', 80, 5, '100', '20', '1000', '10', '10', '2', '20', '37.5', '7', '', 'old-tp-1', 'old-sl-1', '', '', 1, 1)")
        result = scoring.run_round(decision_round_ts=4000)
        records = scoring.recent_reduction_records()
        trade = scoring._connect().execute(f"SELECT take_profit_price, take_profit_order_id FROM {TradingExperiment.TRADES_TABLE} WHERE symbol = 'BANK'").fetchone()

    assert result["reduction_records"] == 1
    assert records[0]["status"] == "submitted"
    assert any(params.get("type") == "TAKE_PROFIT" and params.get("quantity") == "1.5" and params.get("price") == "46.67" for _, params in fake_account.signed_posts)
    assert trade["take_profit_price"] == "46.67"
    assert trade["take_profit_order_id"] != "old-tp-1"


def test_increase_cancels_and_recreates_hard_take_profit_from_actual_position():
    fake_account = PositionChangeHardTakeProfitAccountManager(refreshed_amount="3", refreshed_entry="9")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        scoring.init_tables()
        TradingExperiment(db_path=db_path, account_manager=fake_account).init_tables()
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", [("BANK", 3000, 70), ("BANK", 2000, 73)])
            conn.execute(f"INSERT INTO {TradingExperiment.TRADES_TABLE} (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, stop_loss_calculation, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at) VALUES ('BANK', 1, 'LONG', 'opened', 72, 5, '100', '20', '1000', '10', '7', '2', '20', '34.5', '6', '', 'old-tp-1', 'old-sl-1', '', '', 1000, 1000)")
        positions = [{"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": "8", "entryPrice": "7", "unRealizedProfit": "70", "leverage": "5"}]
        checks = scoring.evaluate_increase_conditions(positions=positions, decision_round_ts=3000, checked_at=4000)
        records = scoring._record_increase_actions(checks, positions=positions, now_ms=4000)
        trade = scoring._connect().execute(f"SELECT take_profit_price, take_profit_order_id, stop_loss_price, stop_loss_order_id FROM {TradingExperiment.TRADES_TABLE} WHERE symbol = 'BANK'").fetchone()

    assert records == 1
    assert ("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "old-tp-1"}) in fake_account.signed_deletes
    assert ("/fapi/v1/algoOrder", {"symbol": "BANKUSDT", "algoId": "old-sl-1"}) in fake_account.signed_deletes
    assert any(params.get("type") == "TAKE_PROFIT" and params.get("quantity") == "3" and params.get("price") == "27.34" for _, params in fake_account.signed_posts)
    assert any(endpoint == "/fapi/v1/order" and params.get("type") == "STOP" and params.get("quantity") == "3" and params.get("price") == "6" and params.get("stopPrice") == "6" for endpoint, params in fake_account.signed_posts)
    assert trade["take_profit_price"] == "27.34"
    assert trade["take_profit_order_id"] != "old-tp-1"
    assert trade["stop_loss_price"] == "6"
    assert trade["stop_loss_order_id"] != "old-sl-1"

class RefreshingMarkAccountManager(FakeAccountManager):
    def __init__(self, mark_price="10"):
        super().__init__()
        self.mark_price = mark_price

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/premiumIndex":
            return {"markPrice": self.mark_price}
        return super()._public_get(endpoint, params)

    def _signed_get(self, endpoint, params=None):
        if endpoint == "/fapi/v3/positionRisk":
            return [{"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": self.mark_price, "unRealizedProfit": "70", "leverage": "5", "entryPrice": "7"}]
        if endpoint == "/fapi/v3/account":
            return {"availableBalance": "5000"}
        return super()._signed_get(endpoint, params)


def _seed_increase_pretrigger_db(db_path: str, scoring: HoldingPositionScoringSystem) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
        conn.execute("CREATE TABLE trading_experiment_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, status TEXT, created_at INTEGER, total_score TEXT, entry_price TEXT, take_profit_order_id TEXT, stop_loss_order_id TEXT, stop_loss_price TEXT)")
        conn.executemany(
            "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
            [("BANK", 3000, 75), ("BANK", 2000, 76)],
        )
        conn.execute(
            "INSERT INTO trading_experiment_trades (symbol, status, created_at, total_score, entry_price, take_profit_order_id, stop_loss_order_id, stop_loss_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", "opened", 1000, "72", "7", "", "", "6"),
        )
    scoring.init_tables()
    TradingExperiment(db_path=db_path, account_manager=scoring.account_manager).init_tables()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT INTO {scoring.REDUCTION_RECORDS_TABLE} (symbol, decision_round_ts, side, matched_rule, reduction_percent, original_quantity, reduced_quantity, remaining_quantity, status, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", 2500, "SELL", "规则三", "0.3", "2", "0.6", "1.4", "submitted", "test", 2000),
        )
        conn.execute(
            f"INSERT INTO {scoring.REDUCTION_CHECKS_TABLE} (symbol, decision_round_ts, highest_15m_high, current_price, price_drawdown_ratio, account_equity_usdt, two_r_usdt, one_r_usdt, unrealized_pnl, open_total_score, latest_total_score, score_drawdown, triggered, tag, reason, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", 2500, "10", "9", "0.1", "5000", "100", "50", "20", "72", "70", "2", 1, "tag", "test", 2000),
        )


def test_position_increase_marks_pretrigger_when_only_condition3_fails():
    fake_account = RefreshingMarkAccountManager(mark_price="8")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        _seed_increase_pretrigger_db(db_path, scoring)
        positions = [{"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": "8", "unRealizedProfit": "70", "leverage": "5"}]

        checks = scoring.evaluate_increase_conditions(positions=positions, decision_round_ts=3000, checked_at=4000)

    assert checks[0].triggered is False
    assert checks[0].tag == "预触发"
    assert checks[0].reason == "condition3_current_price_lt_latest_reduction_price"


def test_refresh_pretrigger_updates_mark_price_and_executes_first_add():
    fake_account = RefreshingMarkAccountManager(mark_price="10")
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        _seed_increase_pretrigger_db(db_path, scoring)
        pretrigger = scoring._evaluate_increase_rules(
            {"symbol": "BANKUSDT", "positionAmt": "2", "markPrice": "8", "unRealizedProfit": "70", "leverage": "5"},
            "BANK",
            3000,
            Decimal("5000"),
            Decimal("50"),
            4000,
        )
        scoring._save_increase_check(pretrigger)

        result = scoring.refresh_pretrigger_increase_checks(now_ms=5000)
        _, checks = scoring.get_latest_increase_checks()
        records = scoring.recent_increase_records()

    assert result["refreshed"] == 1
    assert result["triggered"] == 1
    assert result["records"] == 1
    assert checks[0]["tag"] == "第一次加仓"
    assert checks[0]["current_price"] == "10"
    assert records[0]["increased_quantity"] == "1"
