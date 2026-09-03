import pytest

from hard_take_profit_settings import get_settings, set_settings


def test_hard_take_profit_settings_default_and_persistence(tmp_path):
    db_path = str(tmp_path / "config.db")

    assert get_settings(db_path) == {"profit_ratio": 0.2}
    assert set_settings({"profit_ratio": 0.35}, db_path) == {"profit_ratio": 0.35}
    assert get_settings(db_path) == {"profit_ratio": 0.35}


@pytest.mark.parametrize("value", [0, -0.1, 1.01, float("inf"), "invalid"])
def test_hard_take_profit_settings_reject_invalid_ratio(tmp_path, value):
    with pytest.raises(ValueError):
        set_settings({"profit_ratio": value}, str(tmp_path / "config.db"))
