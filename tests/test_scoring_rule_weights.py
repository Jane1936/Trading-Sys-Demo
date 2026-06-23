from scoring_system import DEFAULT_RULE_SCORE_WEIGHTS, load_rule_score_weights


def test_default_rule_weights_match_current_rule_8_and_10_weights():
    assert DEFAULT_RULE_SCORE_WEIGHTS[8] == 15
    assert DEFAULT_RULE_SCORE_WEIGHTS[10] == 5

    weights = load_rule_score_weights("/tmp/nonexistent-scoring-rule-weights.json")
    assert weights[8] == 15
    assert weights[10] == 5
