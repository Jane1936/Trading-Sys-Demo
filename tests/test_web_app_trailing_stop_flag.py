import web_app


def test_manual_trailing_stop_refresh_is_blocked_when_switch_is_off(monkeypatch):
    monkeypatch.setattr(
        web_app.feature_flags,
        "is_feature_enabled",
        lambda key, db_path: False,
    )

    response = web_app.app.test_client().post("/api/trailing-stop/refresh-pretrigger")

    assert response.status_code == 409
    assert response.get_json() == {"error": "移动追踪止盈规则功能开关已关闭"}
