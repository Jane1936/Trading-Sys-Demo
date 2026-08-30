import position_limit_settings
import web_app


def test_position_limit_settings_persist_and_validate(tmp_path):
    db_path = str(tmp_path / "config.db")
    assert position_limit_settings.get_settings(db_path) == {
        "simulation_max_open_positions": 10,
        "live_max_open_positions": 10,
    }
    assert position_limit_settings.set_settings(
        {"simulation_max_open_positions": 3, "live_max_open_positions": 7}, db_path
    ) == {"simulation_max_open_positions": 3, "live_max_open_positions": 7}
    assert position_limit_settings.get_settings(db_path)["simulation_max_open_positions"] == 3


def test_position_limit_settings_reject_invalid_counts(tmp_path):
    try:
        position_limit_settings.set_settings(
            {"simulation_max_open_positions": 0, "live_max_open_positions": 2},
            str(tmp_path / "config.db"),
        )
    except ValueError as exc:
        assert "1–1000" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_position_limit_settings_api_updates_both_accounts(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "CONFIG_DB_PATH", str(tmp_path / "config.db"))
    client = web_app.app.test_client()

    response = client.put(
        "/api/position-limit-settings",
        json={"simulation_max_open_positions": 4, "live_max_open_positions": 6},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "simulation_max_open_positions": 4,
        "live_max_open_positions": 6,
    }
    assert client.get("/api/position-limit-settings").get_json() == response.get_json()
