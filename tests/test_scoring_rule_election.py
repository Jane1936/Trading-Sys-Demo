import sqlite3

import pytest

from openable_symbol_module import OpenableSymbolModule
from scoring_rule_election import get_settings, set_settings


def _rules(statuses=None):
    statuses = statuses or {}
    return [
        {"rule_id": rule_id, "status": statuses.get(rule_id, "ignored")}
        for rule_id in range(1, 19)
    ]


def _payload(configs=None, mode="any"):
    configs = configs or {}
    return {
        "combination_mode": mode,
        "configurations": [
            {"key": key, "enabled": key in configs,
             "rules": _rules(configs.get(key, {}).get("statuses")),
             "optional_min": configs.get(key, {}).get("optional_min", 0)}
            for key in "ABCDE"
        ],
    }


def test_election_settings_default_to_backwards_compatible_no_requirements(tmp_path):
    settings = get_settings(str(tmp_path / "base.db"))

    assert settings["combination_mode"] == "any"
    assert [config["key"] for config in settings["configurations"]] == list("ABCDE")
    assert settings["configurations"][0]["enabled"] is True
    assert all(len(config["rules"]) == 18 for config in settings["configurations"])


def test_election_settings_validate_optional_min_against_optional_count(tmp_path):
    db_path = str(tmp_path / "base.db")

    with pytest.raises(ValueError, match="number of optional rules"):
        set_settings(_payload({"A": {"statuses": {1: "optional"}, "optional_min": 2}}), db_path)


def test_openable_election_requires_all_required_and_n_optional_rules(tmp_path, monkeypatch):
    election = _payload({"A": {"statuses": {1: "required", 2: "optional", 3: "optional"}, "optional_min": 1}})
    monkeypatch.setattr("openable_symbol_module.get_rule_election_settings", lambda _path: election)
    module = OpenableSymbolModule(db_path=str(tmp_path / "scoring.db"))
    module.init_table()
    rule_columns = ", ".join(f"rule{i}_score INTEGER" for i in range(1, 19))
    placeholders = ", ".join("?" for _ in range(21))
    with module._connect() as conn:
        conn.execute(
            f"CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER, {rule_columns})"
        )
        conn.execute("CREATE TABLE current_round_cooldown_symbols (symbol TEXT, decision_round_ts INTEGER)")
        conn.execute("CREATE TABLE symbol_scores_structural_stop_loss_distance (symbol TEXT, decision_round_ts INTEGER, stop_loss_distance_ratio REAL)")
        rows = [
            ("PASS", 1, 80, 4, 6, 0, *([0] * 15)),
            ("NO_REQUIRED", 1, 80, 0, 6, 0, *([0] * 15)),
            ("NO_OPTIONAL", 1, 80, 4, 0, 0, *([0] * 15)),
        ]
        conn.executemany(f"INSERT INTO symbol_total_scores VALUES ({placeholders})", rows)
        conn.executemany(
            "INSERT INTO symbol_scores_structural_stop_loss_distance VALUES (?, 1, 0.01)",
            [(row[0],) for row in rows],
        )

    results = {row.symbol: row for row in module.run_round(1, evaluated_at=123)}

    assert results["PASS"].qualified is True
    assert results["NO_REQUIRED"].qualified is False
    assert results["NO_REQUIRED"].reason == "scoring_rule_election_not_satisfied:A"
    assert results["NO_OPTIONAL"].qualified is False
    assert results["NO_OPTIONAL"].reason == "scoring_rule_election_not_satisfied:A"


@pytest.mark.parametrize("mode,expected", [("any", True), ("all", False)])
def test_openable_combines_enabled_configurations(tmp_path, monkeypatch, mode, expected):
    election = _payload({
        "A": {"statuses": {1: "required"}},
        "B": {"statuses": {2: "required"}},
    }, mode)
    monkeypatch.setattr("openable_symbol_module.get_rule_election_settings", lambda _path: election)
    module = OpenableSymbolModule(db_path=str(tmp_path / "scoring.db"))
    module.init_table()
    rule_columns = ", ".join(f"rule{i}_score INTEGER" for i in range(1, 19))
    with module._connect() as conn:
        conn.execute(f"CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER, {rule_columns})")
        conn.execute("CREATE TABLE current_round_cooldown_symbols (symbol TEXT, decision_round_ts INTEGER)")
        conn.execute("CREATE TABLE symbol_scores_structural_stop_loss_distance (symbol TEXT, decision_round_ts INTEGER, stop_loss_distance_ratio REAL)")
        conn.execute(f"INSERT INTO symbol_total_scores VALUES ({','.join('?' for _ in range(21))})", ("TEST", 1, 80, 4, *([0] * 17)))
        conn.execute("INSERT INTO symbol_scores_structural_stop_loss_distance VALUES ('TEST', 1, 0.01)")

    assert module.run_round(1, evaluated_at=123)[0].qualified is expected
