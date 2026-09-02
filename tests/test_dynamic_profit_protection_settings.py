from decimal import Decimal

import dynamic_profit_protection_settings
from dynamic_profit_protection import DynamicProfitProtection
import web_app


def _custom_settings():
    return {
        "enabled": True,
        "tier_2_min_r": 2.5,
        "tier_3_min_r": 3.5,
        "tier_4_min_r": 5.0,
        "tier_2_drawdown_ratio": 0.35,
        "tier_3_drawdown_ratio": 0.25,
        "tier_4_drawdown_ratio": 0.15,
    }


def test_settings_persist_with_production_defaults(tmp_path):
    path = str(tmp_path / "config.db")
    assert dynamic_profit_protection_settings.get_settings(path) == {
        "enabled": True,
        "tier_2_min_r": 2.0,
        "tier_3_min_r": 3.0,
        "tier_4_min_r": 4.0,
        "tier_2_drawdown_ratio": 0.4,
        "tier_3_drawdown_ratio": 0.3,
        "tier_4_drawdown_ratio": 0.2,
    }
    assert dynamic_profit_protection_settings.set_settings(_custom_settings(), path) == _custom_settings()


def test_custom_settings_drive_tiers_and_drawdowns():
    settings = _custom_settings()
    tier_for = DynamicProfitProtection._tier_and_threshold_for_reached_r_multiple
    assert tier_for(Decimal("2.5"), settings) == ("未达档", Decimal("0"))
    assert tier_for(Decimal("2.6"), settings) == ("(2.5R, 3.5R]", Decimal("0.35"))
    assert tier_for(Decimal("4"), settings) == ("(3.5R, 5R]", Decimal("0.25"))
    assert tier_for(Decimal("5.1"), settings) == ("5R以上", Decimal("0.15"))


def test_settings_reject_invalid_boundaries_and_drawdowns(tmp_path):
    path = str(tmp_path / "config.db")
    invalid = _custom_settings()
    invalid["tier_3_min_r"] = invalid["tier_2_min_r"]
    try:
        dynamic_profit_protection_settings.set_settings(invalid, path)
    except ValueError as exc:
        assert "严格递增" in str(exc)
    else:
        raise AssertionError("expected invalid tier boundaries to be rejected")


def test_settings_api_is_shared_in_config_database(tmp_path, monkeypatch):
    path = str(tmp_path / "config.db")
    monkeypatch.setattr(web_app, "CONFIG_DB_PATH", path)
    client = web_app.app.test_client()

    response = client.put("/api/dynamic-profit-protection-settings", json=_custom_settings())

    assert response.status_code == 200
    assert response.get_json() == _custom_settings()
    assert client.get("/api/dynamic-profit-protection-settings").get_json() == _custom_settings()
