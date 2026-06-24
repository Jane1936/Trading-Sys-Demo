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
