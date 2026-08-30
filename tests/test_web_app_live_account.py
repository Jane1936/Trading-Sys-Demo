import web_app


class FakeLiveManager:
    def futures_balance(self):
        return {"testnet": False, "base_url": "https://fapi.binance.com", "balances": []}

    def futures_recent_filled_orders(self, days=7, limit=1000):
        return {"testnet": False, "days": days, "orders": []}

    def futures_filled_orders(self, start_time, end_time, limit=1000):
        return {"testnet": False, "start_time": start_time, "end_time": end_time, "orders": []}


def test_live_balance_route_uses_live_manager(monkeypatch):
    monkeypatch.setattr(web_app.BinanceAccountManager, "live", lambda: FakeLiveManager())

    response = web_app.app.test_client().get("/api/live/account/balance")

    assert response.status_code == 200
    assert response.get_json()["testnet"] is False


def test_live_filled_orders_route_supports_days_and_explicit_range(monkeypatch):
    monkeypatch.setattr(web_app.BinanceAccountManager, "live", lambda: FakeLiveManager())
    annotation_paths = []
    original_annotate = web_app._annotate_filled_order_exit_reasons

    def capture_annotation_path(payload, *, trading_db_path=None):
        annotation_paths.append(trading_db_path)
        return original_annotate(payload, trading_db_path=trading_db_path)

    monkeypatch.setattr(web_app, "_annotate_filled_order_exit_reasons", capture_annotation_path)
    client = web_app.app.test_client()

    recent = client.get("/api/live/account/filled-orders?days=15")
    explicit = client.get("/api/live/account/filled-orders?start_time=1000&end_time=2000")

    assert recent.status_code == 200
    assert recent.get_json()["days"] == 15
    assert explicit.status_code == 200
    assert explicit.get_json()["start_time"] == 1000
    assert explicit.get_json()["end_time"] == 2000
    assert annotation_paths == [
        web_app.db_config.REAL_TRADING_DB_PATH,
        web_app.db_config.REAL_TRADING_DB_PATH,
    ]
