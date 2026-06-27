import sqlite3
import tempfile
from pathlib import Path

from holding_position_scoring import HoldingPositionScoringSystem


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



def test_portfolio_risk_displays_scores_and_market_closes_lowest_score_when_total_risk_gt_18():
    fake_account = HighPortfolioRiskAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
                [("BANK", 2000, 5), ("BANK", 1000, 5), ("COIN", 2000, 5), ("COIN", 1000, 5)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 2000, 1), ("BANK", 1000, 1), ("COIN", 2000, 1), ("COIN", 1000, 1)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 2000, 80), ("COIN", 2000, 60)],
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        result = scoring.run_round(decision_round_ts=3000)
        risk = scoring.get_latest_portfolio_risk()
        risk_liquidation_records = scoring.recent_portfolio_liquidation_records()
        stop_loss_records = scoring.recent_stop_loss_records()

    assert result["total_risk"] == "20"
    assert result["portfolio_risk_action"].startswith("submitted_market_close_symbol=COIN")
    assert risk is not None
    assert {row.symbol: row.total_score for row in risk.positions} == {"BANK": "80", "COIN": "60"}
    assert fake_account.signed_posts[-1] == (
        "/fapi/v1/order",
        {"symbol": "COINUSDT", "side": "SELL", "type": "MARKET", "quantity": "200", "reduceOnly": "true", "newOrderRespType": "RESULT"},
    )
    assert len(risk_liquidation_records) == 1
    assert risk_liquidation_records[0]["symbol"] == "COIN"
    assert risk_liquidation_records[0]["reason"].startswith("portfolio_total_risk_gt_18")
    assert stop_loss_records == []


def test_portfolio_risk_closes_all_negative_pnl_positions_when_lowest_score_ties():
    fake_account = TiedLowestScorePortfolioRiskAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE klines_15m (symbol TEXT, open_time INTEGER, close REAL)")
            conn.execute("CREATE TABLE symbol_structural_stop_losses (symbol TEXT, decision_round_ts INTEGER, structural_stop_loss REAL)")
            conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
            conn.executemany(
                "INSERT INTO klines_15m (symbol, open_time, close) VALUES (?, ?, ?)",
                [(symbol, 2000, 5) for symbol in ("BANK", "COIN", "FUND")],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [(symbol, 2000, 1) for symbol in ("BANK", "COIN", "FUND")],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 2000, 60), ("COIN", 2000, 60), ("FUND", 2000, 60)],
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account, realized_pnl_retry_delays=())
        result = scoring.run_round(decision_round_ts=3000)
        risk_liquidation_records = scoring.recent_portfolio_liquidation_records()

    close_orders = [params for endpoint, params in fake_account.signed_posts if endpoint == "/fapi/v1/order"]
    assert result["total_risk"] == "30"
    assert "skipped_non_negative_unrealized_pnl_symbol=BANK; unrealized_pnl=5" in result["portfolio_risk_action"]
    assert "submitted_market_close_symbol=COIN" in result["portfolio_risk_action"]
    assert "submitted_market_close_symbol=FUND" in result["portfolio_risk_action"]
    assert close_orders == [
        {"symbol": "COINUSDT", "side": "SELL", "type": "MARKET", "quantity": "200", "reduceOnly": "true", "newOrderRespType": "RESULT"},
        {"symbol": "FUNDUSDT", "side": "SELL", "type": "MARKET", "quantity": "200", "reduceOnly": "true", "newOrderRespType": "RESULT"},
    ]
    assert {row["symbol"] for row in risk_liquidation_records} == {"COIN", "FUND"}
    assert all("unrealized_pnl=-" in row["reason"] for row in risk_liquidation_records)


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
            conn.execute("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", ("BANK", 4000, 54))
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
    assert checks[0]["highest_15m_high"] == "10"
    assert checks[0]["current_price"] == "8"
    assert checks[0]["two_r_usdt"] == "20"
    assert checks[0]["unrealized_pnl"] == "0"
    assert checks[0]["open_total_score"] == "80"
    assert checks[0]["latest_total_score"] == "54"
    assert checks[0]["score_drawdown"] == "26"


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
                [("BANK", 3000, 10, 8, 8), ("BANK", 2000, 8, 8, 8), ("BANK", 1000, 8, 8, 8)],
            )
            conn.executemany(
                "INSERT INTO symbol_structural_stop_losses (symbol, decision_round_ts, structural_stop_loss) VALUES (?, ?, ?)",
                [("BANK", 3000, 7), ("BANK", 2000, 7)],
            )
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 64), ("BANK", 3000, 70)],
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
    assert checks[0]["latest_total_score"] == "64"
    assert checks[0]["previous_total_score"] == "70"
    assert checks[0]["score_drawdown"] == "16"
    assert checks[0]["rule_name"] == "规则二"


def test_position_reduction_rule_tags_price_leading_deterioration():
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
            conn.executemany(
                "INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
                [("BANK", 4000, 79), ("BANK", 3000, 80)],
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
    assert checks[0]["tag"] == "价格领先恶化"
    assert checks[0]["reason"] == "price_leading_deterioration"
    assert checks[0]["rule_name"] == "规则三"
    assert checks[0]["one_r_usdt"] == "10"
    assert checks[0]["two_r_usdt"] == "20"
    assert checks[0]["latest_total_score"] == "79"
    assert checks[0]["previous_total_score"] == "80"


def test_position_reduction_rule_tags_score_danger_zone():
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
            conn.execute("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", ("BANK", 4000, 29))
            conn.execute(
                "INSERT INTO trading_experiment_trades (symbol, status, total_score, created_at) VALUES (?, ?, ?, ?)",
                ("BANK", "opened", 35, 1000),
            )

        scoring = HoldingPositionScoringSystem(db_path=db_path, account_manager=fake_account)
        result = scoring.run_round(decision_round_ts=4000)
        round_ts, checks = scoring.get_latest_reduction_checks()

    assert result["reduction_checked"] == 1
    assert result["reduction_triggered"] == 1
    assert round_ts == 4000
    assert checks[0]["symbol"] == "BANK"
    assert checks[0]["triggered"] == 1
    assert checks[0]["tag"] == "评分进入危险区"
    assert checks[0]["reason"] == "score_danger_zone"
    assert checks[0]["rule_name"] == "规则四"
    assert checks[0]["latest_total_score"] == "29"


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
            conn.execute("INSERT INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)", ("BANK", 4000, 30))
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
