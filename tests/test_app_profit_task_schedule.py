import inspect

import app


def test_profit_task_runs_protection_strategies_in_order_each_loop():
    source = inspect.getsource(app.start_break_even_take_profit_task)

    assert "for _ in range(5)" not in source
    assert "while True:" in source
    break_even_index = source.index("result = strategy.run_round()")
    partial_take_profit_index = source.index("partial_result = partial_strategy.run_round")
    dynamic_profit_protection_index = source.index(
        "dynamic_result = dynamic_profit_protection.run_round()"
    )
    trailing_stop_flag_index = source.index(
        "feature_flags.is_feature_enabled(feature_flags.TRAILING_STOP)"
    )
    trailing_stop_index = source.index("trailing_result = trailing_stop_tracker.run_round()")

    assert (
        break_even_index
        < partial_take_profit_index
        < dynamic_profit_protection_index
        < trailing_stop_flag_index
        < trailing_stop_index
    )
    assert "time.sleep(60)" in source


def test_first_experiment_refreshes_holding_scoring_after_open(monkeypatch):
    calls = []

    class FakeMarketFilter:
        def __init__(self, db_path):
            pass

        def run_round(self, decision_round_ts):
            return type("Result", (), {
                "allow_new_positions": True,
                "allusdt_delta": "0",
                "btc_delta": "0",
                "reason": "ok",
            })()

    class FakeZombie:
        def __init__(self, db_path):
            pass

        def run_round(self, checked_at):
            return {"checked": 0, "triggered": 0, "records": 0}

    class FakeExperiment:
        def __init__(self, db_path):
            pass

        def run_round(self, rows):
            calls.append(("open", len(rows)))
            return {"opened": 2, "skipped": 0, "reason": "completed"}

    class FakeHoldingScoring:
        def __init__(self, db_path):
            pass

        def run_round(self, decision_round_ts, **kwargs):
            calls.append(("holding", decision_round_ts))
            return {"checked": 2, "risk_position_count": 2}

    monkeypatch.setattr(app, "MarketFilterModule", FakeMarketFilter)
    monkeypatch.setattr(app, "ZombieForceLiquidationModule", FakeZombie)
    monkeypatch.setattr(app, "TradingExperiment", FakeExperiment)
    monkeypatch.setattr(app, "HoldingPositionScoringSystem", FakeHoldingScoring)
    monkeypatch.setattr(app.feature_flags, "is_feature_enabled", lambda name: True)
    openable = type("Openable", (), {"qualified": True})()

    app.run_first_experiment_after_openable_round([openable], 123_000)

    assert calls == [("open", 1), ("holding", 123_000)]
