import web_app


def test_hard_take_profit_settings_api_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "CONFIG_DB_PATH", str(tmp_path / "config.db"))
    client = web_app.app.test_client()

    assert client.get("/api/hard-take-profit-settings").get_json() == {
        "profit_ratio": 0.2
    }
    response = client.put(
        "/api/hard-take-profit-settings", json={"profit_ratio": 0.275}
    )

    assert response.status_code == 200
    assert response.get_json() == {"profit_ratio": 0.275}


def test_hard_take_profit_settings_api_rejects_invalid_ratio(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "CONFIG_DB_PATH", str(tmp_path / "config.db"))

    response = web_app.app.test_client().put(
        "/api/hard-take-profit-settings", json={"profit_ratio": 0}
    )

    assert response.status_code == 400
    assert "大于0%" in response.get_json()["error"]
