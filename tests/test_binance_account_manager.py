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


def test_recent_filled_orders_include_buy_and_sell_then_merge_same_symbol_time_side():
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

    assert len(orders) == 2
    buy_order = next(order for order in orders if order["side"] == "BUY")
    sell_order = next(order for order in orders if order["side"] == "SELL")
    assert buy_order["symbol"] == "BANKUSDT"
    assert buy_order["time"] == 1000
    assert buy_order["order_id"] == "1,2"
    assert buy_order["trade_id"] == "10,11"
    assert buy_order["price"] == "2.5"
    assert buy_order["quantity"] == "4"
    assert buy_order["quote_quantity"] == "10"
    assert buy_order["commission"] == "0.03"
    assert sell_order["order_id"] == "3"
    assert sell_order["realized_pnl"] == "1.5"


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
