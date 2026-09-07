from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_scoring_page_uses_independent_template_script_and_ordered_navigation():
    template = Path("templates/scoring.html").read_text(encoding="utf-8")
    script = Path("static/js/scoring.js").read_text(encoding="utf-8")
    navigation = template[template.index("{% block navigation %}"):template.index("{% endblock %}", template.index("{% block navigation %}"))]

    assert '{% extends "base.html" %}' in template
    assert "filename='js/scoring.js'" in template
    assert navigation.index("返回交易控制台") < navigation.index("异常插针记录") < navigation.index("评分系统") < navigation.index("评分变化趋势") < navigation.index("本轮交易策略")
    for panel in ("tab-abnormal", "tab-score", "tab-score-trend", "tab-strategy"):
        assert f'id="{panel}"' in template
    assert "/api/safety/score-trend" in script
    assert "buildScoreTrendOption" in script


def test_scoring_route_only_calls_scoring_context_loader():
    with (
        patch("web_app.initialize_config_database") as initialize_config,
        patch("web_app.load_scoring_context", return_value={}) as load_context,
        patch("web_app.render_template", return_value="scoring page") as render,
        patch("web_app.load_market_safety_context") as market,
        patch("web_app.load_simulation_context") as simulation,
        patch("web_app.load_live_context") as live,
    ):
        response = app.test_client().get("/strategy/scoring?symbol=BTCUSDT&limit=999")

    assert response.status_code == 200
    initialize_config.assert_called_once()
    load_context.assert_called_once_with()
    render.assert_called_once_with("scoring.html")
    market.assert_not_called()
    simulation.assert_not_called()
    live.assert_not_called()


def test_scoring_page_degrades_when_one_module_fails():
    with (
        patch("web_app.initialize_config_database"),
        patch("web_app.ScoringSystem.get_latest_round_total_scores", side_effect=RuntimeError("scoring database unavailable")),
    ):
        response = app.test_client().get("/strategy/scoring")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "部分模块加载失败" in body
    assert "scoring database unavailable" in body
    assert "异常插针记录" in body


def test_legacy_scoring_links_redirect_and_preserve_query_parameters():
    client = app.test_client()
    for legacy_tab in ("tab-abnormal", "tab-score", "tab-score-trend", "tab-strategy"):
        response = client.get(f"/safety/abnormal-wicks?active_tab={legacy_tab}&source=bookmark&filter=a&filter=b")
        assert response.status_code == 302
        assert response.location == f"/strategy/scoring?active_tab={legacy_tab}&source=bookmark&filter=a&filter=b"
