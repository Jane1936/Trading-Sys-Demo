import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pre_safety_module import Candle5m, PreSafetyModule


def test_abnormal_wick_requires_70_percent_wick_1_percent_span_and_2_5x_body():
    hit, cond1_ratio, cond2_ratio = PreSafetyModule._is_abnormal(
        Candle5m("BANK", 1, 2, 100.0, 110.0, 100.0, 102.0)
    )

    assert hit is True
    assert cond1_ratio == 0.8
    assert cond2_ratio == 0.1


def test_abnormal_wick_rejects_previous_60_percent_and_6_percent_only_rule():
    hit, cond1_ratio, cond2_ratio = PreSafetyModule._is_abnormal(
        Candle5m("BANK", 1, 2, 100.0, 106.0, 100.0, 102.0)
    )

    assert hit is False
    assert cond1_ratio < 0.7
    assert cond2_ratio >= 0.01


def test_detect_for_symbol_records_symbol_when_any_recent_three_closed_5m_candle_matches(tmp_path):
    db_path = tmp_path / "klines.db"
    module = PreSafetyModule(db_path=str(db_path))
    module.init_table()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE klines_5m (
                symbol TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL DEFAULT 0,
                close_time INTEGER NOT NULL,
                PRIMARY KEY(symbol, open_time)
            )
            """
        )
        rows = [
            ("BANK", 1, 100.0, 100.5, 99.5, 100.1, 0, 2),
            ("BANK", 2, 100.0, 110.0, 100.0, 102.0, 0, 3),
            ("BANK", 3, 100.0, 100.4, 99.7, 100.1, 0, 4),
        ]
        conn.executemany(
            "INSERT INTO klines_5m (symbol, open_time, open, high, low, close, volume, close_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    events = module.detect_for_symbol("BANK", now_ms=15 * 60_000)
    _, symbols = module.get_latest_round_abnormal_symbols()

    assert len(events) == 1
    assert events[0].symbol == "BANK"
    assert events[0].candle_index == 2
    assert symbols == ["BANK"]
