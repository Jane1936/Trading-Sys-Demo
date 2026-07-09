from pathlib import Path

import pytest

from scoring_system import DEFAULT_RULE_SCORE_WEIGHTS, load_rule_score_weights


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
