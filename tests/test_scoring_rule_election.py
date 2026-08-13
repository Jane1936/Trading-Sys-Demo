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


def test_election_settings_default_to_backwards_compatible_no_requirements(tmp_path):
    settings = get_settings(str(tmp_path / "base.db"))

    assert settings["optional_min"] == 0
    assert len(settings["rules"]) == 18
    assert {rule["status"] for rule in settings["rules"]} == {"ignored"}


def test_election_settings_validate_optional_min_against_optional_count(tmp_path):
    db_path = str(tmp_path / "base.db")

    with pytest.raises(ValueError, match="number of optional rules"):
        set_settings({"rules": _rules({1: "optional"}), "optional_min": 2}, db_path)


def test_openable_election_requires_all_required_and_n_optional_rules(tmp_path, monkeypatch):
    election = {
        "rules": _rules({1: "required", 2: "optional", 3: "optional"}),
        "optional_min": 1,
    }
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
    assert results["NO_REQUIRED"].reason == "required_scoring_rules_not_hit:1"
    assert results["NO_OPTIONAL"].qualified is False
    assert results["NO_OPTIONAL"].reason == "optional_scoring_rules_not_enough:0/1"

