from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_live_page_uses_independent_template_and_script():
    template = Path("templates/live.html").read_text(encoding="utf-8")
    dashboard = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    script = Path("static/js/live.js").read_text(encoding="utf-8")

    assert '{% extends "base.html" %}' in template
    assert "filename='js/live.js'" in template
    assert 'id="tab-live"' in template
    assert 'id="tab-live"' not in dashboard
    assert "/api/live/account/balance" in script
    assert "live-module-refresh" in script


def test_live_route_only_calls_its_context_loader():
    with (
        patch("web_app.initialize_config_database") as initialize_config,
        patch("web_app.load_live_context", return_value={}) as load_context,
        patch("web_app.render_template", return_value="live page") as render,
        patch("web_app.PreSafetyModule") as safety,
        patch("web_app.ScoringSystem") as scoring,
        patch("web_app.TradingExperiment") as simulation,
    ):
        response = app.test_client().get("/trading/live?symbol=BTCUSDT")

    assert response.status_code == 200
    initialize_config.assert_called_once()
    load_context.assert_called_once_with()
    render.assert_called_once_with("live.html")
    safety.assert_not_called()
    scoring.assert_not_called()
    simulation.assert_not_called()


def test_live_page_degrades_when_one_module_fails():
    with (
        patch("web_app.initialize_config_database"),
        patch("web_app.real_trading.initialize"),
        patch("web_app.real_trading.experiment") as experiment,
        patch("web_app.real_trading.holding_scoring") as holding,
        patch("web_app.real_trading.high_frequency_modules", return_value=()),
        patch("web_app._score_band_context", return_value=([], "", "", 0)),
    ):
        experiment.return_value.recent_trade_records.side_effect = RuntimeError("live database unavailable")
        experiment.return_value.latest_position_snapshots.return_value = []
        experiment.return_value.recent_error_records.return_value = []
        holding.return_value.get_latest_round_checks.return_value = (0, [])
        holding.return_value.get_latest_reduction_checks.return_value = (0, [])
        holding.return_value.get_latest_increase_checks.return_value = (0, [])
        holding.return_value.get_latest_portfolio_risk.return_value = None
        holding.return_value.latest_pretrigger_increase_rounds.return_value = {}
        holding.return_value.recent_stop_loss_records.return_value = []
        holding.return_value.recent_reduction_records.return_value = []
        holding.return_value.recent_reduction_stop_failure_liquidations.return_value = []
        holding.return_value.recent_increase_records.return_value = []
        response = app.test_client().get("/trading/live")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "部分模块加载失败" in body
    assert "live database unavailable" in body


def test_legacy_live_tab_redirects_and_preserves_query_parameters():
    response = app.test_client().get(
        "/safety/abnormal-wicks?active_tab=tab-live&source=bookmark&filter=a&filter=b"
    )

    assert response.status_code == 302
    assert response.location == "/trading/live?source=bookmark&filter=a&filter=b"
