from binance_account_manager import BinanceAccountManager


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
