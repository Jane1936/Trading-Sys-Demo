import sqlite3

from data_processor import MA20Processor, init_ema_table, save_ema_result


def _adjust_false_ema(values, span):
    alpha = 2 / (span + 1)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1 - alpha))
    return ema


def test_latest_15m_result_includes_ema16_and_ema21_adjust_false(tmp_path):
    db_path = tmp_path / "klines.db"
    closes = [float(i) for i in range(1, 26)]
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE klines_15m (
                symbol TEXT,
                open_time INTEGER,
                close_time INTEGER,
                close REAL
            )
            """)
        conn.executemany(
            "INSERT INTO klines_15m (symbol, open_time, close_time, close) VALUES (?, ?, ?, ?)",
            [
                ("BTCUSDT", i * 900_000, (i + 1) * 900_000 - 1, close)
                for i, close in enumerate(closes)
            ],
        )

    result = MA20Processor(db_path=str(db_path)).get_latest_ma20("BTCUSDT", "15m")

    assert result is not None
    assert result.ema16 == _adjust_false_ema(closes, 16)
    assert result.ema21 == _adjust_false_ema(closes, 21)


def test_save_ema_result_persists_15m_ema_values(tmp_path):
    db_path = tmp_path / "klines.db"
    init_ema_table(str(db_path))

    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE klines_15m (
                symbol TEXT,
                open_time INTEGER,
                close_time INTEGER,
                close REAL
            )
            """)
        conn.executemany(
            "INSERT INTO klines_15m (symbol, open_time, close_time, close) VALUES (?, ?, ?, ?)",
            [
                ("BTCUSDT", i * 900_000, (i + 1) * 900_000 - 1, float(i + 1))
                for i in range(25)
            ],
        )

    result = MA20Processor(db_path=str(db_path)).get_latest_ma20("BTCUSDT", "15m")
    assert result is not None

    save_ema_result(str(db_path), result)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT symbol, interval, open_time, close, ema16, ema21 FROM ema_indicators"
        ).fetchone()

    assert row == ("BTCUSDT", "15m", 24 * 900_000, 25.0, result.ema16, result.ema21)
