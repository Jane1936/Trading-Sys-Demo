from pathlib import Path

import pytest

from scoring_system import (
    DEFAULT_RULE_SCORE_WEIGHTS,
    ScoringSystem,
    get_rule_score_weight_settings,
    load_rule_score_weights,
    set_rule_score_weight_settings,
)


def test_default_rule_weights_match_current_rule_weights():
    assert DEFAULT_RULE_SCORE_WEIGHTS[8] == 15
    assert DEFAULT_RULE_SCORE_WEIGHTS[10] == 5
    assert DEFAULT_RULE_SCORE_WEIGHTS[12] == 8
    assert DEFAULT_RULE_SCORE_WEIGHTS[13] == 3

    weights = load_rule_score_weights("/tmp/nonexistent-scoring-rule-weights.json")
    assert weights[8] == 15
    assert weights[10] == 5
    assert weights[12] == 8
    assert weights[13] == 3


def test_load_rule_weights_accepts_hand_edited_json_with_comments_and_trailing_commas(tmp_path: Path):
    config_path = tmp_path / "scoring_rule_weights.json"
    config_path.write_text(
        """
        {
          // 手动调权时常见的注释不会再导致评分系统启动失败
          "rules": {
            "8": 12,
            "10": 7, # 行尾注释也支持
          },
        }
        """,
        encoding="utf-8",
    )

    weights = load_rule_score_weights(config_path)

    assert weights[8] == 12
    assert weights[10] == 7
    assert weights[1] == DEFAULT_RULE_SCORE_WEIGHTS[1]


def test_load_rule_weights_still_rejects_unknown_rule_ids(tmp_path: Path):
    config_path = tmp_path / "scoring_rule_weights.json"
    config_path.write_text('{"rules": {"99": 1}}', encoding="utf-8")

    with pytest.raises(ValueError, match="Unknown scoring rule id"):
        load_rule_score_weights(config_path)


def test_runtime_rule_weights_are_seeded_updated_and_loaded_by_scoring_system(tmp_path: Path):
    settings_db = str(tmp_path / "base.db")
    scoring_db = str(tmp_path / "scoring.db")

    seeded = get_rule_score_weight_settings(settings_db)
    assert len(seeded) == 18
    assert seeded[7]["weight"] == 15

    updated = DEFAULT_RULE_SCORE_WEIGHTS.copy()
    updated[8] = 22
    set_rule_score_weight_settings(updated, settings_db)

    scoring = ScoringSystem(scoring_db, settings_db_path=settings_db)
    assert scoring.rule_score_weights[8] == 22


def test_runtime_rule_weights_skip_schema_lock_when_current(tmp_path: Path, monkeypatch):
    settings_db = str(tmp_path / "base.db")
    seeded = get_rule_score_weight_settings(settings_db)
    assert len(seeded) == 18

    class UnexpectedSchemaLock:
        def __init__(self, db_path: str):
            self.db_path = db_path

        def __enter__(self):
            raise AssertionError("current scoring weights should not take schema lock")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("scoring_system.db_config.sqlite_schema_lock", UnexpectedSchemaLock)

    current = get_rule_score_weight_settings(settings_db)
    assert len(current) == 18
    assert current[7]["weight"] == DEFAULT_RULE_SCORE_WEIGHTS[8]


@pytest.mark.parametrize("invalid_weight", [-1, 101, 1.5, True, "5"])
def test_runtime_rule_weights_reject_invalid_values(tmp_path: Path, invalid_weight):
    weights = DEFAULT_RULE_SCORE_WEIGHTS.copy()
    weights[1] = invalid_weight

    with pytest.raises(ValueError, match="weight must"):
        set_rule_score_weight_settings(weights, str(tmp_path / "base.db"))
