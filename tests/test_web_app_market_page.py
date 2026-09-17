from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_market_page_uses_independent_template_and_script():
    template = Path("templates/market.html").read_text(encoding="utf-8")
    dashboard = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    script = Path("static/js/market.js").read_text(encoding="utf-8")

    assert '{% extends "base.html" %}' in template
    assert "filename='js/market.js'" in template
    assert 'id="tab-btc"' in template
    assert 'id="tab-market-filter"' in template
    assert 'id="tab-btc"' not in dashboard
    assert 'id="tab-market-filter"' not in dashboard
    assert "/api/btc/5m" in script
    assert "renderBtcKlineChart" in script
    assert 'id="allusdt-24h-highlight"' in template
    assert "/api/market/allusdt-24h" in script
    assert "15 * 60 * 1000" in script


def test_allusdt_24h_ticker_api_returns_normalized_change_and_refresh_time():
    with patch("web_app.requests.get") as get:
        get.return_value.json.return_value = {"priceChangePercent": "-2.345"}

        response = app.test_client().get("/api/market/allusdt-24h")

    assert response.status_code == 200
    assert response.get_json()["symbol"] == "ALLUSDT"
    assert response.get_json()["price_change_percent"] == -2.345
    assert response.get_json()["refreshed_at"] > 0
    get.assert_called_once_with(
        "https://fapi.binance.com/fapi/v1/ticker/24hr",
        params={"symbol": "ALLUSDT"},
        timeout=(3, 10),
    )


def test_allusdt_24h_ticker_api_reports_invalid_upstream_payload():
    with patch("web_app.requests.get") as get:
        get.return_value.json.return_value = {}

        response = app.test_client().get("/api/market/allusdt-24h")

    assert response.status_code == 502
    assert "ALLUSDT 24h ticker request failed" in response.get_json()["error"]


def test_market_page_has_separate_sidebar_tabs_and_only_btc_is_initially_visible():
    template = Path("templates/market.html").read_text(encoding="utf-8")
    script = Path("static/js/market.js").read_text(encoding="utf-8")

    navigation_start = template.index("{% block navigation %}")
    navigation = template[navigation_start:template.index("{% endblock %}", navigation_start)]
    assert (
        navigation.index("返回交易控制台")
        < navigation.index("BTC数据")
        < navigation.index("市场行情过滤")
    )
    assert 'data-market-tab="tab-btc"' in navigation
    assert 'data-market-tab="tab-market-filter"' in navigation
    assert '<section id="tab-btc" class="panel active" role="tabpanel">' in template
    assert '<section id="tab-market-filter" class="panel" role="tabpanel" hidden>' in template
    assert "selectMarketTab(window.location.hash.slice(1) || 'tab-btc', false)" in script


def test_market_route_only_calls_its_context_loader():
    with (
        patch("web_app.initialize_config_database") as initialize_config,
        patch("web_app.load_market_safety_context", return_value={}) as load_context,
        patch("web_app.render_template", return_value="market page") as render,
        patch("web_app.PreSafetyModule") as pre_safety,
        patch("web_app.ScoringSystem") as scoring,
        patch("web_app.TradingExperiment") as trading,
        patch("web_app.real_trading") as live,
    ):
        response = app.test_client().get("/safety/market?symbol=BTCUSDT&limit=999")

    assert response.status_code == 200
    initialize_config.assert_called_once()
    load_context.assert_called_once_with()
    render.assert_called_once_with("market.html")
    pre_safety.assert_not_called()
    scoring.assert_not_called()
    trading.assert_not_called()
    live.assert_not_called()


def test_market_page_degrades_when_one_module_fails():
    with (
        patch("web_app.initialize_config_database"),
        patch(
            "web_app.MarketFilterModule.recent_results",
            side_effect=RuntimeError("market database unavailable"),
        ),
    ):
        response = app.test_client().get("/safety/market")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "部分模块加载失败" in body
    assert "market database unavailable" in body
    assert "BTC数据" in body


def test_market_page_uses_configured_allusdt_24h_threshold_in_filter_copy():
    context = {
        "module_errors": [],
        "active_tab": "tab-market",
        "btc_5m_rows": [],
        "btc_page": 1,
        "btc_page_size": 24,
        "btc_total_rows": 0,
        "btc_total_pages": 1,
        "market_filter_results": [],
        "weak_market_profit_adjustment_results": [],
        "add_position_permission_results": [],
        "dynamic_add_position_threshold_results": [],
        "dynamic_open_threshold_results": [],
        "dynamic_open_threshold_errors": [],
        "market_filter_settings": {
            "btc_siphon_threshold": 0.005,
            "market_crash_threshold": 0.03,
            "allusdt_24h_drop_threshold": -0.0825,
            "block_duration_minutes": 30,
        },
        "weak_market_profit_settings": {
            "trigger_r_multiple": 1.4,
            "take_profit_fraction": 0.5,
        },
    }
    with (
        patch("web_app.initialize_config_database"),
        patch("web_app.load_market_safety_context", return_value=context),
    ):
        response = app.test_client().get("/safety/market")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "ALLUSDT 最近24小时涨跌幅&lt;-8.25%" in body
    assert "24h跌破-8.25%" in body
    assert "24h跌破-5%" not in body


def test_legacy_market_tabs_redirect_and_preserve_query_parameters():
    client = app.test_client()
    for legacy_tab in ("tab-btc", "tab-market-filter"):
        response = client.get(
            f"/safety/abnormal-wicks?active_tab={legacy_tab}&source=bookmark&filter=a&filter=b"
        )

        assert response.status_code == 302
        assert response.location == "/safety/market?source=bookmark&filter=a&filter=b"
