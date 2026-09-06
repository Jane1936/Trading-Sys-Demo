from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from web_app import app


def test_settings_page_uses_independent_template_and_script():
    template = Path("templates/settings.html").read_text(encoding="utf-8")
    dashboard = Path("templates/abnormal_wicks.html").read_text(encoding="utf-8")
    script = Path("static/js/settings.js").read_text(encoding="utf-8")

    assert '{% extends "base.html" %}' in template
    assert "filename='js/settings.js'" in template
    assert 'id="feature-flag-rows"' in template
    assert 'id="tab-feature-flags"' not in dashboard
    assert "initFeatureFlagToggles" in script
    assert "initOpenableSettingsForm" in script


def test_settings_route_only_calls_its_context_loader():
    with (
        patch("web_app.initialize_config_database") as initialize_config,
        patch("web_app.load_settings_context", return_value={}) as load_context,
        patch("web_app.render_template", return_value="settings page") as render,
        patch("web_app.PreSafetyModule") as pre_safety,
        patch("web_app.ScoringSystem") as scoring,
        patch("web_app.TradingExperiment") as trading,
    ):
        response = app.test_client().get("/settings")

    assert response.status_code == 200
    initialize_config.assert_called_once()
    load_context.assert_called_once_with()
    render.assert_called_once_with("settings.html")
    pre_safety.assert_not_called()
    scoring.assert_not_called()
    trading.assert_not_called()


def test_settings_page_degrades_when_one_loader_fails():
    with (
        patch("web_app.initialize_config_database"),
        patch(
            "web_app.feature_flags.list_feature_flags",
            side_effect=RuntimeError("feature flags unavailable"),
        ),
    ):
        response = app.test_client().get("/settings")

    assert response.status_code == 200
    assert "部分模块加载失败" in response.get_data(as_text=True)
    assert "feature flags unavailable" in response.get_data(as_text=True)


def test_legacy_settings_tab_redirects_and_preserves_other_query_parameters():
    response = app.test_client().get(
        "/safety/abnormal-wicks?active_tab=tab-feature-flags&source=bookmark&filter=a&filter=b"
    )

    assert response.status_code == 302
    assert response.location == "/settings?source=bookmark&filter=a&filter=b"
