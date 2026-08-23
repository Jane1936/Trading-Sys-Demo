import inspect

import app


class _NeverConvergedAdjustment:
    def __init__(self):
        self.calls = 0

    def is_data_converged_for_round(self, decision_round_ts):
        self.calls += 1
        return False, "waiting_allusdt_15m_convergence"


def test_profit_market_convergence_wait_is_bounded():
    adjustment = _NeverConvergedAdjustment()

    converged, reason = app.wait_for_profit_market_convergence(
        adjustment, 123_000, timeout_sec=0, poll_sec=0.01
    )

    assert converged is False
    assert reason == "waiting_allusdt_15m_convergence"
    assert adjustment.calls == 1


def test_profit_market_convergence_returns_as_soon_as_inputs_are_ready(monkeypatch):
    responses = iter(
        [
            (False, "waiting_allusdt_15m_convergence"),
            (True, "data_converged"),
        ]
    )

    class Adjustment:
        def is_data_converged_for_round(self, decision_round_ts):
            return next(responses)

    monkeypatch.setattr(app.time, "sleep", lambda seconds: None)
    converged, reason = app.wait_for_profit_market_convergence(
        Adjustment(), 123_000, timeout_sec=10, poll_sec=0.01
    )

    assert converged is True
    assert reason == "data_converged"


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


def test_profit_task_relies_on_process_startup_schema_barrier():
    source = inspect.getsource(app.start_break_even_take_profit_task)

    assert ".init_tables()" not in source
    assert ".init_table()" not in source
    assert "profit protection task initialization failed" not in source


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
