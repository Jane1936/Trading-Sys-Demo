import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app


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
