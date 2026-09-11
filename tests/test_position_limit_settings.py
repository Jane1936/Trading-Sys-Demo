import sqlite3

import position_limit_settings
import web_app


def test_position_limit_settings_persist_and_validate(tmp_path):
    db_path = str(tmp_path / "config.db")
    assert position_limit_settings.get_settings(db_path) == {
        "simulation_max_open_positions": 10,
        "live_max_open_positions": 10,
        "max_new_positions_per_round": 5,
    }
    assert position_limit_settings.set_settings(
        {
            "simulation_max_open_positions": 3,
            "live_max_open_positions": 7,
            "max_new_positions_per_round": 4,
        },
        db_path,
    ) == {
        "simulation_max_open_positions": 3,
        "live_max_open_positions": 7,
        "max_new_positions_per_round": 4,
    }
    assert position_limit_settings.get_settings(db_path)["simulation_max_open_positions"] == 3


def test_position_limit_settings_reject_invalid_counts(tmp_path):
    try:
        position_limit_settings.set_settings(
            {
                "simulation_max_open_positions": 0,
                "live_max_open_positions": 2,
                "max_new_positions_per_round": 5,
            },
            str(tmp_path / "config.db"),
        )
    except ValueError as exc:
        assert "1–1000" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_position_limit_settings_adds_round_limit_to_existing_table(tmp_path):
    db_path = str(tmp_path / "config.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE position_limit_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                simulation_max_open_positions INTEGER NOT NULL,
                live_max_open_positions INTEGER NOT NULL,
                updated_at INTEGER NOT NULL)"""
        )
        conn.execute("INSERT INTO position_limit_settings VALUES (1, 3, 7, 1)")

    assert position_limit_settings.get_settings(db_path) == {
        "simulation_max_open_positions": 3,
        "live_max_open_positions": 7,
        "max_new_positions_per_round": 5,
    }


def test_position_limit_settings_api_updates_both_accounts(tmp_path, monkeypatch):
    monkeypatch.setattr(web_app, "CONFIG_DB_PATH", str(tmp_path / "config.db"))
    client = web_app.app.test_client()

    response = client.put(
        "/api/position-limit-settings",
        json={
            "simulation_max_open_positions": 4,
            "live_max_open_positions": 6,
            "max_new_positions_per_round": 2,
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "simulation_max_open_positions": 4,
        "live_max_open_positions": 6,
        "max_new_positions_per_round": 2,
    }
    assert client.get("/api/position-limit-settings").get_json() == response.get_json()
