import tempfile
from decimal import Decimal
from pathlib import Path

from partial_take_profit import PartialTakeProfitStrategy
from trading_experiment import TradingExperiment
from trailing_stop_tracker import TrailingStopTracker


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
                    "positionAmt": "10",
                    "entryPrice": "10",
                    "markPrice": "12.5",
                    "unRealizedProfit": "25",
                    "leverage": "5",
                    "notional": "125",
                    "liquidationPrice": "0",
                }
            ]
        raise AssertionError(f"unexpected signed endpoint {endpoint}")

    def _public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/price":
            return {"price": "12.5"}
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

    def _signed_delete(self, endpoint, params=None):
        self.signed_deletes.append((endpoint, dict(params or {})))
        return {"status": "CANCELED"}

    def _signed_post(self, endpoint, params=None):
        self.signed_posts.append((endpoint, dict(params or {})))
        return {"orderId": 456}

def _insert_1m_kline(db_path, high, open_time, close=12):
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS klines_1m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                funding_rate REAL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO klines_1m (symbol, open_time, open, high, low, close, volume, close_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", open_time, 10, high, 9, close, 100, open_time + 59999),
        )


def _insert_15m_kline(db_path, low, open_time, close=12):
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS klines_15m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                funding_rate REAL,
                PRIMARY KEY (symbol, open_time)
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO klines_15m (symbol, open_time, open, high, low, close, volume, close_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("BANK", open_time, 10, 14, low, close, 100, open_time + 899999),
        )


def _insert_atr14(db_path, atr14):
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS atr_15m_indicators (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                atr14 REAL NOT NULL,
                PRIMARY KEY(symbol, open_time)
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO atr_15m_indicators (symbol, open_time, atr14) VALUES (?, ?, ?)",
            ("BANK", 1000, atr14),
        )


def _insert_total_score(db_path, total_score):
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_total_scores (
                symbol TEXT NOT NULL,
                decision_round_ts INTEGER NOT NULL,
                total_score INTEGER NOT NULL,
                PRIMARY KEY(symbol, decision_round_ts)
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO symbol_total_scores (symbol, decision_round_ts, total_score) VALUES (?, ?, ?)",
            ("BANK", 1000, total_score),
        )


def _insert_open_trade(db_path):
    experiment = TradingExperiment(db_path=db_path, account_manager=FakeAccountManager())
    experiment.init_tables()
    with experiment._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {TradingExperiment.TRADES_TABLE}
            (symbol, decision_round_ts, side, status, total_score, leverage, allocated_usdt, required_margin_usdt, account_equity_usdt, max_loss_usdt, entry_price, quantity, notional_usdt, take_profit_price, stop_loss_price, take_profit_order_id, stop_loss_order_id, reason, raw_response, created_at, updated_at)
            VALUES ('BANK', 1000, 'LONG', 'opened', 80, 5, '100', '20', '5100', '51', '10', '10', '100', '15', '9', '123', '321', 'test', '{{}}', 1000, 1000)
            """
        )


def _insert_partial_take_profit_record(db_path):
    strategy = PartialTakeProfitStrategy(db_path=db_path, account_manager=FakeAccountManager())
    strategy.init_tables()
    with strategy._connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {PartialTakeProfitStrategy.RECORDS_TABLE}
            (symbol, checked_at, side, position_amt, take_profit_quantity, entry_price, account_equity_usdt, r_usdt, trigger_r_usdt, unrealized_pnl, take_profit_order_id, status, reason, raw_response)
            VALUES ('BANK', 1000, 'SELL', '10', '3', '10', '5100', '51', '102', '110', '789', 'submitted', 'unrealized_pnl_ge_2r_take_profit_30_percent', '{{}}')
            """
        )


def test_trailing_stop_tracker_updates_max_after_partial_take_profit():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        _insert_15m_kline(db_path, low=13, open_time=3000)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        first = tracker.run_round()
        _insert_1m_kline(db_path, high=12, open_time=2000)
        second = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert first["eligible"] == 1
    assert first["updated"] == 1
    assert second["eligible"] == 1
    assert checks[0].eligible is True
    assert checks[0].unrealized_pnl_at_high == "20"
    assert checks[0].max_unrealized_pnl == "30"
    assert checks[0].current_profit_drawdown == "0.3333333333333333333333333333"
    assert "max_unrealized_pnl_unchanged" in checks[0].reason


def test_trailing_stop_tracker_requires_partial_take_profit_record():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["eligible"] == 0
    assert checks[0].eligible is False
    assert checks[0].reason == "partial_take_profit_not_triggered"


def test_trailing_stop_tracker_closes_position_when_drawdown_threshold_hit():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_open_trade(db_path)
        _insert_total_score(db_path, total_score=80)
        _insert_atr14(db_path, atr14=0.25)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        _insert_15m_kline(db_path, low=13, open_time=3000)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        tracker.run_round()
        _insert_1m_kline(db_path, high=12.3, open_time=2000, close=12.3)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()
        action_records = tracker.recent_action_records()

    assert result["eligible"] == 1
    assert checks[0].trailing_stop_triggered is True
    assert checks[0].drawdown_threshold == "0.625"
    assert checks[0].current_mark_price == "12.3"
    assert checks[0].latest_15m_low == "13"
    assert checks[0].pretriggered is True
    assert checks[0].tag == "移动追踪止盈"
    assert checks[0].atr14 == "0.25"
    assert checks[0].volatility == "0.02083333333333333333333333333"
    assert checks[0].price_drawdown == "0.7"
    assert checks[0].cancel_take_profit_order_id == "123"
    assert checks[0].cancel_status == "submitted"
    assert checks[0].close_quantity == "10"
    assert checks[0].close_order_id == "456"
    assert checks[0].close_status == "submitted"
    assert len(action_records) == 1
    assert action_records[0].close_order_id == "456"
    assert fake_account.signed_deletes == [
        ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/allOpenOrders", {"symbol": "BANKUSDT"}),
        ("/fapi/v1/algoOpenOrders", {"symbol": "BANKUSDT"}),
    ]
    assert fake_account.signed_posts == [
        ("/fapi/v1/order", {"symbol": "BANKUSDT", "side": "SELL", "type": "MARKET", "quantity": "10", "reduceOnly": "true", "newOrderRespType": "RESULT"})
    ]


def test_trailing_stop_tracker_does_not_close_when_latest_close_is_below_long_entry():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_open_trade(db_path)
        _insert_atr14(db_path, atr14=0.25)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        _insert_15m_kline(db_path, low=13, open_time=3000)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        tracker.run_round()
        _insert_1m_kline(db_path, high=9.9, open_time=2000, close=9.9)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["eligible"] == 1
    assert checks[0].pretriggered is True
    assert checks[0].price_drawdown == "3.1"
    assert checks[0].trailing_stop_triggered is False
    assert checks[0].close_status == "not_required"
    assert "latest_1m_close_not_in_profit" in checks[0].reason
    assert fake_account.signed_deletes == []
    assert fake_account.signed_posts == []


def test_trailing_stop_tracker_requires_pretrigger_before_close():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_open_trade(db_path)
        _insert_total_score(db_path, total_score=80)
        _insert_atr14(db_path, atr14=0.25)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        _insert_15m_kline(db_path, low=13, open_time=3000)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        tracker.run_round()
        _insert_1m_kline(db_path, high=12.3, open_time=2000, close=12.3)
        _insert_15m_kline(db_path, low=12.4, open_time=5000, close=13)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["eligible"] == 1
    assert checks[0].trailing_stop_triggered is False
    assert checks[0].close_status == "not_required"
    assert checks[0].current_mark_price == "12.3"
    assert checks[0].latest_15m_low == "12.4"
    assert checks[0].pretriggered is False
    assert checks[0].tag == ""
    assert "latest_15m_close_gte_min_prev_three_lows" in checks[0].reason
    assert fake_account.signed_deletes == []
    assert fake_account.signed_posts == []




def test_refresh_pretriggered_symbols_updates_latest_close_highest_and_drawdown():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_open_trade(db_path)
        _insert_atr14(db_path, atr14=0.25)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000)
        _insert_15m_kline(db_path, low=13, open_time=2000)
        _insert_15m_kline(db_path, low=13, open_time=3000)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        tracker.run_round()
        _insert_1m_kline(db_path, high=14, open_time=2000, close=13.4)
        payload = tracker.refresh_pretriggered_symbols()
        _, checks = tracker.get_latest_round_checks()

    assert payload["refreshed"] == 1
    assert payload["triggered"] == 0
    assert checks[0].pretriggered is True
    assert checks[0].tag == "预触发移动追踪止盈"
    assert checks[0].kline_high == "14"
    assert checks[0].latest_1m_close == "13.4"
    assert checks[0].highest_since_open == "14"
    assert checks[0].price_drawdown == "0.6"
    assert checks[0].drawdown_threshold == "0.625"
    assert checks[0].trailing_stop_triggered is False


def test_trailing_stop_tracker_uses_2atr_threshold_when_volatility_is_high():
    fake_account = FakeAccountManager()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "klines.db")
        _insert_partial_take_profit_record(db_path)
        _insert_open_trade(db_path)
        _insert_atr14(db_path, atr14=0.5)
        _insert_1m_kline(db_path, high=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=1000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=2000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=3000, close=13)
        _insert_15m_kline(db_path, low=13, open_time=4000, close=12)
        tracker = TrailingStopTracker(db_path=db_path, account_manager=fake_account)

        tracker.run_round()
        _insert_1m_kline(db_path, high=12.3, open_time=2000, close=11.9)
        result = tracker.run_round()
        _, checks = tracker.get_latest_round_checks()

    assert result["eligible"] == 1
    assert checks[0].volatility == "0.03846153846153846153846153846"
    assert checks[0].drawdown_threshold == "1"
    assert checks[0].price_drawdown == "1.1"
    assert checks[0].trailing_stop_triggered is True
    assert "drawdown_gt_2atr" in checks[0].reason
