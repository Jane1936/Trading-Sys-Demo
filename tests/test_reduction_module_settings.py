import pytest

from reduction_module_settings import get_settings, set_settings


def test_reduction_settings_default_and_persist(tmp_path):
    db_path = str(tmp_path / "config.db")
    assert get_settings(db_path)["rule2"]["reduction_fraction"] == 0.25
    assert get_settings(db_path)["rule5"]["reduction_fraction"] == 0.5

    result = set_settings(
        {
            "rule2": {"enabled": False, "reduction_fraction": 0.4},
            "rule5": {"enabled": True, "reduction_fraction": 0.65},
        },
        db_path,
    )

    assert result["rule2"]["enabled"] is False
    assert result["rule2"]["reduction_fraction"] == 0.4
    assert get_settings(db_path)["rule5"]["reduction_fraction"] == 0.65


@pytest.mark.parametrize("fraction", [0, -0.1, 1.01, float("inf")])
def test_reduction_settings_reject_invalid_fraction(tmp_path, fraction):
    with pytest.raises(ValueError):
        set_settings(
            {
                "rule2": {"enabled": True, "reduction_fraction": fraction},
                "rule5": {"enabled": True, "reduction_fraction": 0.5},
            },
            str(tmp_path / "config.db"),
        )
