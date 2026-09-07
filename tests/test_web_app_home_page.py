from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_home_page_is_rendered_instead_of_redirecting_to_scoring():
    with patch("web_app.render_template", return_value="home page") as render:
        response = app.test_client().get("/")

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "home page"
    render.assert_called_once_with("home.html")


def test_home_page_navigation_contains_all_standalone_workspaces():
    response = app.test_client().get("/")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    expected_links = (
        ("/trading/live", "实盘"),
        ("/trading/simulation", "模拟盘"),
        ("/settings", "系统配置"),
        ("/safety/market", "市场安全"),
        ("/strategy/scoring", "评分系统与开仓"),
    )
    for path, label in expected_links:
        assert f'href="{path}"' in body
        assert label in body


def test_standalone_pages_return_to_home_page():
    for template_name in ("live.html", "simulation.html", "settings.html", "market.html", "scoring.html"):
        template = Path("templates", template_name).read_text(encoding="utf-8")
        navigation = template.split("{% block navigation %}", 1)[1].split("{% endblock %}", 1)[0]
        assert "url_for('index')" in navigation
        assert "返回交易控制台" in navigation
