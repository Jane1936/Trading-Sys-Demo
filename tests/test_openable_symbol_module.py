from openable_symbol_module import OpenableSymbolModule


def test_low_trial_band_starts_at_67():
    assert OpenableSymbolModule.MIN_TOTAL_SCORE == 67
    assert OpenableSymbolModule.score_band_for_total(66) == "NA"
    assert OpenableSymbolModule.score_band_for_total(67) == "低档试错单"
    assert OpenableSymbolModule.score_band_for_total(72) == "低档试错单"


def test_low_trial_threshold_and_leverage_are_config_driven():
    assert OpenableSymbolModule.distance_threshold_for_total(66) is None
    assert OpenableSymbolModule.distance_threshold_for_total(67) == 0.05
    assert OpenableSymbolModule.opening_leverage_for_total_and_distance(67, 0.02, "A档") == "4x"
    assert OpenableSymbolModule.opening_leverage_for_total_and_distance(66, 0.02, "A档") == "NA"


def test_zero_distance_ratio_has_no_default_leverage_for_any_openable_band():
    assert OpenableSymbolModule.stop_loss_distance_tier_for_ratio(0) == "NA"
    assert OpenableSymbolModule.opening_leverage_for_total_and_distance(67, 0, "A档") == "NA"
    assert OpenableSymbolModule.opening_leverage_for_total_and_distance(81, 0, "A档") == "NA"


def test_zero_distance_ratio_is_not_final_openable_for_low_trial(tmp_path):
    db_path = tmp_path / "klines.db"
    module = OpenableSymbolModule(db_path=str(db_path))
    module.init_table()
    with module._connect() as conn:
        conn.execute("CREATE TABLE symbol_total_scores (symbol TEXT, decision_round_ts INTEGER, total_score INTEGER)")
        conn.execute("CREATE TABLE current_round_cooldown_symbols (symbol TEXT, decision_round_ts INTEGER)")
        conn.execute("CREATE TABLE symbol_scores_structural_stop_loss_distance (symbol TEXT, decision_round_ts INTEGER, stop_loss_distance_ratio REAL)")
        conn.execute("INSERT INTO symbol_total_scores VALUES ('LOW', 1, 67)")
        conn.execute("INSERT INTO symbol_scores_structural_stop_loss_distance VALUES ('LOW', 1, 0)")

    rows = module.run_round(1, evaluated_at=123)

    assert len(rows) == 1
    assert rows[0].opening_leverage == "NA"
    assert rows[0].distance_qualified is False
    assert rows[0].qualified is False
    assert rows[0].reason == "zero_distance_ratio_not_openable"
