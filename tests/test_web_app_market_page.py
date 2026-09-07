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


def test_legacy_market_tabs_redirect_and_preserve_query_parameters():
    client = app.test_client()
    for legacy_tab in ("tab-btc", "tab-market-filter"):
        response = client.get(
            f"/safety/abnormal-wicks?active_tab={legacy_tab}&source=bookmark&filter=a&filter=b"
        )

        assert response.status_code == 302
        assert response.location == "/safety/market?source=bookmark&filter=a&filter=b"
