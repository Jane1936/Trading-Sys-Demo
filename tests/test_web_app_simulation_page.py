from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_simulation_route_only_calls_its_context_loader():
    with (
        patch("web_app.initialize_config_database") as initialize_config,
        patch("web_app.load_simulation_context", return_value={}) as load_context,
        patch("web_app.render_template", return_value="simulation page") as render,
        patch("web_app.PreSafetyModule") as safety,
        patch("web_app.ScoringSystem") as scoring,
        patch("web_app.real_trading") as live,
    ):
        response = app.test_client().get("/trading/simulation?symbol=BTCUSDT")

    assert response.status_code == 200
    initialize_config.assert_called_once()
    load_context.assert_called_once_with()
    render.assert_called_once_with("simulation.html")
    safety.assert_not_called()
    scoring.assert_not_called()
    live.assert_not_called()


def test_simulation_page_degrades_when_one_module_fails():
    with (
        patch("web_app.initialize_config_database"),
        patch("web_app.TradingExperiment.recent_trade_records", side_effect=RuntimeError("paper database unavailable")),
    ):
        response = app.test_client().get("/trading/simulation")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "部分模块加载失败" in body
    assert "paper database unavailable" in body


def test_simulation_page_serializes_equity_trend_rows_from_its_context_loader():
    equity_rows = [
        {"recorded_at": 1_725_523_200_000, "account_equity_usdt": 1000.0},
        {"recorded_at": 1_725_609_600_000, "account_equity_usdt": 1012.5},
    ]
    with (
        patch("web_app.initialize_config_database"),
        patch("web_app._experiment_equity_trend_rows", return_value=equity_rows),
    ):
        response = app.test_client().get("/trading/simulation")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'id="simulation-equity-trend-data"' in body
    assert "[1725523200000, 1000.0]" in body
    assert "[1725609600000, 1012.5]" in body


def test_legacy_simulation_tab_redirects_and_preserves_query_parameters():
    response = app.test_client().get(
        "/safety/abnormal-wicks?active_tab=tab-simulation&source=bookmark&filter=a&filter=b"
    )

    assert response.status_code == 302
    assert response.location == "/trading/simulation?source=bookmark&filter=a&filter=b"
