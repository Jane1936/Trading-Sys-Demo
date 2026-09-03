from dataclasses import dataclass

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


def test_live_filled_orders_route_returns_json_for_unexpected_annotation_error(monkeypatch):
    monkeypatch.setattr(web_app.BinanceAccountManager, "live", lambda: FakeLiveManager())
    monkeypatch.setattr(
        web_app,
        "_annotate_filled_order_exit_reasons",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("database unavailable")),
    )

    response = web_app.app.test_client().get("/api/live/account/filled-orders?days=7")

    assert response.status_code == 500
    assert response.is_json
    assert response.get_json() == {
        "error": "Unexpected live filled-orders query failure: database unavailable"
    }


class FakeLiveModule:
    def get_latest_round_checks(self):
        return 1234, [{"symbol": "BTC", "reason": "latest"}]

    def recent_records(self, limit):
        return [{"symbol": "BTC", "status": "submitted"}]

    def recent_action_records(self, limit):
        return [{"symbol": "BTC", "status": "submitted"}]


class FakeLiveExperiment:
    def latest_position_snapshots(self, limit):
        return [{"symbol": "BTC", "position_amt": "1", "updated_at": 1200}]


def test_live_high_frequency_summary_reads_only_requested_module(monkeypatch):
    modules = [FakeLiveModule() for _ in range(6)]
    monkeypatch.setattr(web_app.real_trading, "high_frequency_modules", lambda: tuple(modules))
    monkeypatch.setattr(web_app.real_trading, "experiment", lambda: FakeLiveExperiment())

    response = web_app.app.test_client().get(
        "/api/live/high-frequency/dynamic-profit-protection/summary"
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert {key: payload[key] for key in ("key", "label", "round_ts", "checks", "records")} == {
        "key": "dynamic-profit-protection",
        "label": "动态利润保护",
        "round_ts": 1234,
        "checks": [{"symbol": "BTC", "reason": "latest"}],
        "records": [{"symbol": "BTC", "status": "submitted"}],
    }
    assert len(payload["tables"]["check_columns"]) == 19
    assert len(payload["tables"]["record_columns"]) == 18


def test_live_hard_take_profit_summary_exposes_matching_tables(monkeypatch):
    modules = [FakeLiveModule() for _ in range(6)]
    monkeypatch.setattr(web_app.real_trading, "high_frequency_modules", lambda: tuple(modules))
    monkeypatch.setattr(web_app.real_trading, "experiment", lambda: FakeLiveExperiment())

    response = web_app.app.test_client().get(
        "/api/live/high-frequency/hard-take-profit/summary"
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["label"] == "硬止盈"
    assert [column[1] for column in payload["tables"]["check_columns"]] == [
        "checked_at", "symbol", "entry_price", "position_amt", "unrealized_pnl",
        "position_notional", "profit_ratio", "profit_threshold", "triggered",
        "close_order_id", "close_status", "reason",
    ]


def test_unknown_live_high_frequency_module_is_rejected():
    response = web_app.app.test_client().get("/api/live/high-frequency/unknown/summary")

    assert response.status_code == 404


def test_live_module_checks_follow_current_positions_and_mark_new_symbols_pending():
    checks = [{"symbol": "BTC", "checked_at": 1000, "reason": "scanned"}]
    snapshots = [
        {"symbol": "BTC", "position_amt": "1", "updated_at": 1200},
        {"symbol": "ETH", "position_amt": "2", "updated_at": 1200},
    ]

    synchronized = web_app._sync_live_module_checks(checks, snapshots)

    assert [row["symbol"] for row in synchronized] == ["BTC", "ETH"]
    assert synchronized[0]["reason"] == "scanned"
    assert synchronized[1] == {
        "symbol": "ETH",
        "position_amt": "2",
        "updated_at": 1200,
        "decision_round_ts": 1200,
        "checked_at": 1200,
        "calculated_at": 1200,
        "current_price": "",
        "unrealized_pnl": "",
        "one_r_usdt": "",
        "latest_total_score": "",
        "previous_total_score": "",
        "latest_reduction_price": "",
        "open_trade_created_at": None,
        "latest_15m_open_time": None,
        "latest_15m_close": None,
        "latest_structural_stop_loss": None,
        "prev_15m_open_time": None,
        "prev_15m_close": None,
        "prev_structural_stop_loss": None,
        "open_entry_price": "",
        "ema16": "",
        "ema21": "",
        "score_drawdown": "",
        "atr14": "",
        "two_r_usdt": "",
        "open_total_score": "",
        "latest_15m_open": "",
        "second_15m_open": "",
        "second_15m_close": "",
        "third_15m_open": "",
        "third_15m_close": "",
        "latest_macd": "",
        "second_macd": "",
        "third_macd": "",
        "rule_name": "",
        "tag": "新开仓待扫描",
        "status": "待扫描",
        "triggered": False,
        "reason": "该symbol在模块最近一轮扫描后新开仓，等待下一轮扫描",
    }


def test_live_module_checks_drop_symbols_without_a_current_position():
    checks = [
        {"symbol": "BTC", "reason": "still open"},
        {"symbol": "SOL", "reason": "already closed"},
    ]
    snapshots = [{"symbol": "BTC", "position_amt": "1", "updated_at": 1200}]

    synchronized = web_app._sync_live_module_checks(checks, snapshots)

    assert synchronized == [{"symbol": "BTC", "reason": "still open"}]


def test_live_portfolio_risk_adds_new_positions_as_pending():
    @dataclass
    class Summary:
        decision_round_ts: int = 1000
        total_risk: str = "0.5"
        position_count: int = 1
        account_equity_usdt: str = "100"
        calculated_at: int = 1000
        positions: list = None

    summary = Summary()
    summary.positions = []
    snapshots = [{"symbol": "ETH", "position_amt": "2", "mark_price": "20", "updated_at": 1200}]

    synchronized = web_app._sync_live_portfolio_risk(summary, snapshots)

    assert synchronized["position_count"] == 1
    assert synchronized["pending_count"] == 1
    assert synchronized["positions"][0]["symbol"] == "ETH"
    assert synchronized["positions"][0]["position_amt"] == "2"
    assert synchronized["positions"][0]["reason"] == "该symbol在模块最近一轮扫描后新开仓，等待下一轮扫描"
