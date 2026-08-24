import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db_config
from binance_account_manager import BinanceAccountConfigError, BinanceAccountManager


class FakeRecentTradesManager(BinanceAccountManager):
    def __init__(self, rows):
        super().__init__(api_key="key", secret_key="secret")
        self.rows = rows

    def validate_config(self):
        return None

    def _signed_get(self, endpoint, params=None):
        assert endpoint == "/fapi/v1/userTrades"
        return self.rows


def test_recent_filled_orders_include_buy_and_sell_then_keep_distinct_order_ids():
    manager = FakeRecentTradesManager(
        [
            {
                "symbol": "BANKUSDT",
                "orderId": 1,
                "id": 10,
                "time": 1000,
                "buyer": True,
                "price": "2",
                "qty": "3",
                "quoteQty": "6",
                "realizedPnl": "0",
                "commission": "0.01",
                "commissionAsset": "USDT",
                "maker": True,
            },
            {
                "symbol": "BANKUSDT",
                "orderId": 2,
                "id": 11,
                "time": 1000,
                "buyer": True,
                "price": "4",
                "qty": "1",
                "quoteQty": "4",
                "realizedPnl": "0",
                "commission": "0.02",
                "commissionAsset": "USDT",
                "maker": True,
            },
            {
                "symbol": "BANKUSDT",
                "orderId": 3,
                "id": 12,
                "time": 1000,
                "buyer": False,
                "price": "5",
                "qty": "2",
                "quoteQty": "10",
                "realizedPnl": "1.5",
                "commission": "0.03",
                "commissionAsset": "USDT",
                "maker": False,
            },
        ]
    )

    orders = manager.futures_recent_filled_orders(days=7)["orders"]

    assert len(orders) == 3
    buy_orders = sorted((order for order in orders if order["side"] == "BUY"), key=lambda order: order["order_id"])
    sell_order = next(order for order in orders if order["side"] == "SELL")
    assert buy_orders[0]["symbol"] == "BANKUSDT"
    assert buy_orders[0]["time"] == 1000
    assert buy_orders[0]["order_id"] == "1"
    assert buy_orders[0]["trade_id"] == "10"
    assert buy_orders[0]["price"] == "2"
    assert buy_orders[0]["quantity"] == "3"
    assert buy_orders[0]["quote_quantity"] == "6"
    assert buy_orders[0]["commission"] == "0.01"
    assert buy_orders[1]["order_id"] == "2"
    assert buy_orders[1]["quantity"] == "1"
    assert sell_order["order_id"] == "3"
    assert sell_order["realized_pnl"] == "1.5"


def test_recent_filled_orders_merge_same_order_id_across_fill_times():
    manager = FakeRecentTradesManager(
        [
            {
                "symbol": "BANKUSDT",
                "orderId": 9,
                "id": 20,
                "time": 2000,
                "buyer": False,
                "price": "2",
                "qty": "300",
                "quoteQty": "600",
                "realizedPnl": "-1",
                "commission": "0.01",
                "commissionAsset": "USDT",
            },
            {
                "symbol": "BANKUSDT",
                "orderId": 9,
                "id": 21,
                "time": 2500,
                "buyer": False,
                "price": "3",
                "qty": "400",
                "quoteQty": "1200",
                "realizedPnl": "-2",
                "commission": "0.02",
                "commissionAsset": "USDT",
            },
        ]
    )

    orders = manager.futures_recent_filled_orders(days=7)["orders"]

    assert len(orders) == 1
    assert orders[0]["order_id"] == "9"
    assert orders[0]["trade_id"] == "20,21"
    assert orders[0]["time"] == 2000
    assert orders[0]["price"] == "2.571428571428571428571428571"
    assert orders[0]["quantity"] == "700"
    assert orders[0]["quote_quantity"] == "1800"
    assert orders[0]["realized_pnl"] == "-3"
    assert orders[0]["commission"] == "0.03"


def test_recent_filled_orders_paginates_when_first_user_trades_page_is_full(monkeypatch):
    class PagingRecentTradesManager(BinanceAccountManager):
        def __init__(self):
            super().__init__(api_key="key", secret_key="secret")
            self.calls = []

        def validate_config(self):
            return None

        def _signed_get(self, endpoint, params=None):
            assert endpoint == "/fapi/v1/userTrades"
            self.calls.append(dict(params or {}))
            if len(self.calls) == 1:
                return [
                    {
                        "symbol": "BANKUSDT",
                        "orderId": 1,
                        "id": 10,
                        "time": 1000,
                        "buyer": False,
                        "price": "1",
                        "qty": "1",
                    },
                    {
                        "symbol": "BANKUSDT",
                        "orderId": 2,
                        "id": 11,
                        "time": 2000,
                        "buyer": False,
                        "price": "1",
                        "qty": "1",
                    },
                ]
            return [
                {
                    "symbol": "BANKUSDT",
                    "orderId": 3,
                    "id": 12,
                    "time": 3000,
                    "buyer": False,
                    "price": "1",
                    "qty": "1",
                },
            ]

    monkeypatch.setattr("binance_account_manager.time.time", lambda: 10)
    manager = PagingRecentTradesManager()

    orders = manager.futures_recent_filled_orders(days=1, limit=2)["orders"]

    assert [order["time"] for order in orders] == [3000, 2000, 1000]
    assert len(manager.calls) == 2
    assert manager.calls[0]["startTime"] == 10000 - 24 * 60 * 60 * 1000
    assert manager.calls[0]["endTime"] == 10000
    assert manager.calls[0]["limit"] == 2
    assert manager.calls[1]["startTime"] == 2001
    assert manager.calls[1]["endTime"] == 10000
    assert manager.calls[1]["limit"] == 2


def test_recent_filled_orders_splits_user_trades_requests_into_seven_day_windows(monkeypatch):
    class WindowedRecentTradesManager(BinanceAccountManager):
        def __init__(self):
            super().__init__(api_key="key", secret_key="secret")
            self.calls = []

        def validate_config(self):
            return None

        def _signed_get(self, endpoint, params=None):
            assert endpoint == "/fapi/v1/userTrades"
            self.calls.append(dict(params or {}))
            return []

    day_ms = 24 * 60 * 60 * 1000
    monkeypatch.setattr("binance_account_manager.time.time", lambda: 30 * day_ms / 1000)
    manager = WindowedRecentTradesManager()

    orders = manager.futures_recent_filled_orders(days=15)["orders"]

    assert orders == []
    assert len(manager.calls) == 3
    assert manager.calls[0]["startTime"] == 15 * day_ms
    assert manager.calls[0]["endTime"] == 22 * day_ms
    assert manager.calls[1]["startTime"] == 22 * day_ms + 1
    assert manager.calls[1]["endTime"] == 29 * day_ms + 1
    assert manager.calls[2]["startTime"] == 29 * day_ms + 2
    assert manager.calls[2]["endTime"] == 30 * day_ms
    assert all(call["endTime"] - call["startTime"] <= 7 * day_ms for call in manager.calls)


def test_filled_orders_supports_explicit_start_and_end_times():
    manager = FakeRecentTradesManager([])

    payload = manager.futures_filled_orders(start_time=1_000, end_time=3_600_000, limit=50)

    assert payload["start_time"] == 1_000
    assert payload["end_time"] == 3_600_000
    assert "days" not in payload


def test_filled_orders_rejects_reversed_explicit_range():
    manager = FakeRecentTradesManager([])

    try:
        manager.futures_filled_orders(start_time=2_000, end_time=1_000)
    except ValueError as exc:
        assert str(exc) == "start_time must be earlier than end_time"
    else:
        raise AssertionError("Expected a reversed time range to be rejected")


class FakeResponse:
    text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


@pytest.mark.parametrize(
    ("method_name", "request_method"),
    [
        ("_signed_get", "get"),
        ("_signed_post", "post"),
        ("_signed_delete", "delete"),
        ("_public_get", "get"),
    ],
)
def test_binance_requests_reject_active_sqlite_transactions(
    tmp_path, monkeypatch, method_name, request_method
):
    manager = BinanceAccountManager(api_key="key", secret_key="secret")
    network_calls = []

    def request(*args, **kwargs):
        network_calls.append((args, kwargs))
        return FakeResponse()

    monkeypatch.setattr(manager.session, request_method, request)
    with db_config.connect_sqlite(str(tmp_path / "trading.db")) as conn:
        conn.execute("CREATE TABLE events (value INTEGER)")
        conn.execute("INSERT INTO events VALUES (1)")
        with pytest.raises(RuntimeError, match="active SQLite transaction"):
            getattr(manager, method_name)("/test")
        assert network_calls == []


def test_binance_request_runs_after_sqlite_transaction_commits(tmp_path, monkeypatch):
    manager = BinanceAccountManager(api_key="key", secret_key="secret")
    monkeypatch.setattr(manager.session, "get", lambda *args, **kwargs: FakeResponse())

    with db_config.connect_sqlite(str(tmp_path / "trading.db")) as conn:
        conn.execute("CREATE TABLE events (value INTEGER)")
        conn.execute("INSERT INTO events VALUES (1)")
    assert manager._public_get("/test") == {"ok": True}


def test_live_manager_always_uses_production_credentials(monkeypatch):
    monkeypatch.setenv("BINANCE_REAL_API_KEY", "live-key")
    monkeypatch.setenv("BINANCE_REAL_API_SECRET", "live-secret")
    monkeypatch.setenv("BINANCE_REAL_BASE_URL", "https://live.example")

    manager = BinanceAccountManager.live()

    assert manager.base_url == "https://live.example"
    assert manager.api_key == "live-key"
    assert manager.secret_key == "live-secret"
    assert manager.testnet is False


@pytest.mark.parametrize(
    "base_url",
    ["https://demo-fapi.binance.com", "https://testnet.binancefuture.com/fapi"],
)
def test_live_manager_rejects_demo_endpoint_before_request(monkeypatch, base_url):
    monkeypatch.setenv("BINANCE_REAL_API_KEY", "live-key")
    monkeypatch.setenv("BINANCE_REAL_API_SECRET", "live-secret")
    monkeypatch.setenv("BINANCE_REAL_BASE_URL", base_url)
    manager = BinanceAccountManager.live()
    network_calls = []
    monkeypatch.setattr(manager.session, "get", lambda *args, **kwargs: network_calls.append((args, kwargs)))

    with pytest.raises(BinanceAccountConfigError, match="Demo/Testnet endpoint"):
        manager.futures_balance()

    assert network_calls == []
