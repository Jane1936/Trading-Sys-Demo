import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app
from dynamic_open_threshold import DynamicOpenThresholdModule


class FakeScoringSystem:
    seen_paths = []

    def __init__(self, db_path):
        self.db_path = db_path
        self.seen_paths.append(db_path)

    def init_table(self):
        return None

    def wait_for_15m_ma20_readiness_for_round(self, decision_round_ts, symbols, retries, retry_delay_seconds):
        class Readiness:
            ready = True
            ready_symbols = list(symbols)
            missing_symbols = []
            target_open_time = decision_round_ts

        return Readiness()

    def score_round(self, decision_round_ts, all_symbols, abnormal_symbols):
        return []


class FakeOpenableSymbolModule:
    seen_paths = []

    def __init__(self, db_path):
        self.db_path = db_path
        self.seen_paths.append(db_path)

    def init_table(self):
        return None

    def run_round(self, **kwargs):
        return []


class FakeDynamicOpenThresholdModule:
    seen_paths = []

    def __init__(self, db_path):
        self.db_path = db_path
        self.seen_paths.append(db_path)

    def init_table(self):
        return None

    def run_round(self, **kwargs):
        class Result:
            highest_total_score = 0
            min_open_total_score = 0
            allow_new_positions = False
            policy = "test"
            reason = "test"

        return Result()


class FakeTradingOwnedModule:
    seen_paths = []

    def __init__(self, db_path):
        self.db_path = db_path
        self.seen_paths.append(db_path)

    def init_tables(self):
        return None

    def run_round(self, decision_round_ts):
        return {}


def test_scoring_worker_keeps_scoring_tables_in_scoring_db_and_holding_tables_in_trading_db(monkeypatch, tmp_path):
    scoring_db = str(tmp_path / "scoring.db")
    trading_db = str(tmp_path / "trading.db")
    monkeypatch.setattr(app.db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(app, "ScoringSystem", FakeScoringSystem)
    monkeypatch.setattr(app, "OpenableSymbolModule", FakeOpenableSymbolModule)
    monkeypatch.setattr(app, "DynamicOpenThresholdModule", FakeDynamicOpenThresholdModule)
    monkeypatch.setattr(app, "HoldingPositionScoringSystem", FakeTradingOwnedModule)
    monkeypatch.setattr(app, "TrailingReductionTracker", FakeTradingOwnedModule)
    FakeScoringSystem.seen_paths = []
    FakeOpenableSymbolModule.seen_paths = []
    FakeDynamicOpenThresholdModule.seen_paths = []
    FakeTradingOwnedModule.seen_paths = []

    app.run_scoring_round_worker(
        db_path=scoring_db,
        decision_round_ts=123,
        symbols=["BTC"],
        abnormal_symbols=[],
        evaluated_at=456,
    )

    assert FakeScoringSystem.seen_paths == [scoring_db]
    assert FakeOpenableSymbolModule.seen_paths == [scoring_db]
    assert FakeDynamicOpenThresholdModule.seen_paths == [scoring_db]
    assert FakeTradingOwnedModule.seen_paths == [trading_db, trading_db]


def test_scoring_worker_continues_when_trading_module_initialization_fails(
    monkeypatch, tmp_path
):
    scoring_db = str(tmp_path / "scoring.db")
    score_calls = []
    threshold_calls = []

    class RecordingScoringSystem(FakeScoringSystem):
        def score_round(self, decision_round_ts, all_symbols, abnormal_symbols):
            score_calls.append((decision_round_ts, list(all_symbols)))
            return []

    class RecordingThreshold(FakeDynamicOpenThresholdModule):
        def run_round(self, **kwargs):
            threshold_calls.append(kwargs)
            return super().run_round(**kwargs)

    class BrokenTradingModule(FakeTradingOwnedModule):
        def init_tables(self):
            raise RuntimeError("trading database unavailable")

    monkeypatch.setattr(app, "ScoringSystem", RecordingScoringSystem)
    monkeypatch.setattr(app, "OpenableSymbolModule", FakeOpenableSymbolModule)
    monkeypatch.setattr(app, "DynamicOpenThresholdModule", RecordingThreshold)
    monkeypatch.setattr(app, "HoldingPositionScoringSystem", BrokenTradingModule)
    monkeypatch.setattr(app, "TrailingReductionTracker", FakeTradingOwnedModule)

    app.run_scoring_round_worker(
        db_path=scoring_db,
        decision_round_ts=123,
        symbols=["BTC"],
        abnormal_symbols=[],
        evaluated_at=456,
    )

    assert score_calls == [(123, ["BTC"])]
    assert threshold_calls == [{"decision_round_ts": 123, "evaluated_at": 456}]


def test_scoring_worker_deadline_is_ten_minutes():
    assert app.SCORING_WORKER_DEADLINE_MS == 10 * 60_000


def test_scoring_worker_should_stop_after_deadline():
    assert app._scoring_worker_should_stop(
        decision_round_ts=1,
        deadline_ts=1_000,
        stage="test",
        now_ms=lambda: 1_000,
    )
    assert not app._scoring_worker_should_stop(
        decision_round_ts=1,
        deadline_ts=1_000,
        stage="test",
        now_ms=lambda: 999,
    )


def test_completed_scoring_round_publishes_threshold_openable_and_holding_after_deadline(
    monkeypatch, tmp_path
):
    scoring_db = str(tmp_path / "scoring.db")
    trading_db = str(tmp_path / "trading.db")
    run_calls = []
    threshold_calls = []
    openable_calls = []

    class RecordingThreshold(FakeDynamicOpenThresholdModule):
        def run_round(self, **kwargs):
            threshold_calls.append(kwargs)
            return super().run_round(**kwargs)

    class RecordingOpenable(FakeOpenableSymbolModule):
        def run_round(self, **kwargs):
            openable_calls.append(kwargs)
            return []

    class RecordingTradingModule(FakeTradingOwnedModule):
        def run_round(self, decision_round_ts, **kwargs):
            run_calls.append(decision_round_ts)
            return {}

    monkeypatch.setattr(app.db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(app, "ScoringSystem", FakeScoringSystem)
    monkeypatch.setattr(app, "OpenableSymbolModule", RecordingOpenable)
    monkeypatch.setattr(app, "DynamicOpenThresholdModule", RecordingThreshold)
    monkeypatch.setattr(app, "HoldingPositionScoringSystem", RecordingTradingModule)
    monkeypatch.setattr(app, "TrailingReductionTracker", FakeTradingOwnedModule)
    monkeypatch.setattr(
        app,
        "_scoring_worker_should_stop",
        lambda **kwargs: kwargs["stage"] == "before_first_experiment",
    )

    app.run_scoring_round_worker(
        db_path=scoring_db,
        decision_round_ts=123,
        symbols=["BTC"],
        abnormal_symbols=[],
        evaluated_at=456,
        # The simulated deadline elapses after score_round publication but
        # before the optional trading experiment.
        deadline_ts=1,
    )

    assert run_calls == [123]
    assert threshold_calls == [{"decision_round_ts": 123, "evaluated_at": 456}]
    assert openable_calls == [
        {
            "decision_round_ts": 123,
            "evaluated_at": 456,
            "min_total_score": 0,
            "allow_new_positions": False,
            "threshold_reason": "test",
        }
    ]


def test_scoring_worker_uses_no_dynamic_minimum_when_threshold_result_is_unavailable(
    monkeypatch, tmp_path
):
    scoring_db = str(tmp_path / "scoring.db")
    market_db = str(tmp_path / "market.db")
    openable_calls = []

    class BrokenThreshold(DynamicOpenThresholdModule):
        def run_round(self, **kwargs):
            raise RuntimeError("threshold query unavailable")

    class RecordingOpenable(FakeOpenableSymbolModule):
        def run_round(self, **kwargs):
            openable_calls.append(kwargs)
            return []

    monkeypatch.setattr(app, "ScoringSystem", FakeScoringSystem)
    monkeypatch.setattr(app.db_config, "MARKET_DB_PATH", market_db)
    monkeypatch.setattr(app, "OpenableSymbolModule", RecordingOpenable)
    monkeypatch.setattr(app, "DynamicOpenThresholdModule", BrokenThreshold)
    monkeypatch.setattr(app, "HoldingPositionScoringSystem", FakeTradingOwnedModule)
    monkeypatch.setattr(app, "TrailingReductionTracker", FakeTradingOwnedModule)

    app.run_scoring_round_worker(
        db_path=scoring_db,
        decision_round_ts=123,
        symbols=["BTC"],
        abnormal_symbols=[],
        evaluated_at=456,
    )

    assert openable_calls == [
        {
            "decision_round_ts": 123,
            "evaluated_at": 456,
            "min_total_score": None,
            "allow_new_positions": True,
            "threshold_reason": "dynamic_threshold_unavailable_no_minimum",
        }
    ]
    errors = DynamicOpenThresholdModule.recent_errors(
        error_db_path=market_db,
        now_ms=456,
    )
    assert [row.error for row in errors] == ["threshold query unavailable"]


class FakeActiveProcess:
    pid = 4321

    def __init__(self, alive=True, exitcode=None, alive_after_terminate=False):
        self._alive = alive
        self.exitcode = exitcode
        self.alive_after_terminate = alive_after_terminate
        self.join_calls = []
        self.terminated = False
        self.killed = False

    def is_alive(self):
        return self._alive

    def join(self, timeout=0):
        self.join_calls.append(timeout)
        if self.terminated and not self.alive_after_terminate:
            self._alive = False
            self.exitcode = -15
        if self.killed:
            self._alive = False
            self.exitcode = -9

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


def test_scoring_worker_deadline_sets_force_terminate_grace():
    process = FakeActiveProcess(alive=True)

    next_process, next_round, next_deadline, terminate_after = app._reap_or_timeout_scoring_worker(
        process=process,
        round_ts=10_000,
        deadline_ts=20_000,
        terminate_after_ts=None,
        now_ms=20_001,
    )

    assert next_process is process
    assert next_round == 10_000
    assert next_deadline is None
    assert terminate_after == 20_001 + app.SCORING_WORKER_TERMINATE_GRACE_MS
    assert not process.terminated


def test_scoring_worker_stuck_after_grace_is_cleared():
    process = FakeActiveProcess(alive=True)

    next_process, next_round, next_deadline, terminate_after = app._reap_or_timeout_scoring_worker(
        process=process,
        round_ts=10_000,
        deadline_ts=None,
        terminate_after_ts=30_000,
        now_ms=30_000,
    )

    assert next_process is None
    assert next_round is None
    assert next_deadline is None
    assert terminate_after is None
    assert process.terminated
    assert process.exitcode == -15
