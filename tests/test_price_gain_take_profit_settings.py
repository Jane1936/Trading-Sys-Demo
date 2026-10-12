from price_gain_take_profit_settings import get_settings, set_settings


def test_price_gain_take_profit_settings_have_independent_defaults(tmp_path):
    settings = get_settings(str(tmp_path / "config.db"))
    assert settings == {"simulation_profit_ratio": 0.2, "live_profit_ratio": 0.2}


def test_price_gain_take_profit_settings_persist_simulation_and_live_values_separately(tmp_path):
    path = str(tmp_path / "config.db")
    assert set_settings({"simulation_profit_ratio": 0.35, "live_profit_ratio": 0.45}, path) == {
        "simulation_profit_ratio": 0.35,
        "live_profit_ratio": 0.45,
    }
    assert get_settings(path) == {
        "simulation_profit_ratio": 0.35,
        "live_profit_ratio": 0.45,
    }


def test_price_gain_take_profit_settings_reject_invalid_values(tmp_path):
    path = str(tmp_path / "config.db")
    for payload in (
        {"simulation_profit_ratio": 0, "live_profit_ratio": 0.2},
        {"simulation_profit_ratio": 0.2, "live_profit_ratio": 1.01},
        {"simulation_profit_ratio": 0.2},
    ):
        try:
            set_settings(payload, path)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid price-gain settings should be rejected")
