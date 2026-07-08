import sqlite3

from data_processor import (
    MA20Processor,
    init_ema_table,
    init_macd_table,
    save_ema_result,
    save_macd_result,
)


def _adjust_false_ema(values, span):
    alpha = 2 / (span + 1)
    ema = values[0]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (1 - alpha))
    return ema


def _adjust_false_macd(values):
    ema12 = values[0]
    ema26 = values[0]
    dea = 0.0
    for index, value in enumerate(values):
        if index == 0:
            dif = 0.0
            dea = dif
            continue
        ema12 = (value * (2 / 13)) + (ema12 * (1 - (2 / 13)))
        ema26 = (value * (2 / 27)) + (ema26 * (1 - (2 / 27)))
        dif = ema12 - ema26
        dea = (dif * (2 / 10)) + (dea * (1 - (2 / 10)))
    return dif, dea, 2 * (dif - dea)


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
    assert result.ema12 == _adjust_false_ema(closes, 12)
    assert result.ema16 == _adjust_false_ema(closes, 16)
    assert result.ema21 == _adjust_false_ema(closes, 21)
    assert result.ema26 == _adjust_false_ema(closes, 26)
    assert (
        result.macd_dif,
        result.macd_dea,
        result.macd_histogram,
    ) == _adjust_false_macd(closes)


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
        row = conn.execute("""
            SELECT symbol, interval, open_time, close, ema12, ema16, ema21, ema26
            FROM ema_indicators
            """).fetchone()

    assert row == (
        "BTCUSDT",
        "15m",
        24 * 900_000,
        25.0,
        result.ema12,
        result.ema16,
        result.ema21,
        result.ema26,
    )


def test_save_macd_result_persists_15m_macd_values(tmp_path):
    db_path = tmp_path / "klines.db"
    init_macd_table(str(db_path))

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
                for i in range(35)
            ],
        )

    result = MA20Processor(db_path=str(db_path)).get_latest_ma20("BTCUSDT", "15m")
    assert result is not None

    save_macd_result(str(db_path), result)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT symbol, interval, open_time, close, dif, dea, macd FROM macd_indicators"
        ).fetchone()

    assert row == (
        "BTCUSDT",
        "15m",
        34 * 900_000,
        35.0,
        result.macd_dif,
        result.macd_dea,
        result.macd_histogram,
    )


def test_init_ema_table_adds_ema12_and_ema26_to_existing_table(tmp_path):
    db_path = tmp_path / "klines.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE ema_indicators (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                close REAL NOT NULL,
                ema16 REAL NOT NULL,
                ema21 REAL NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (symbol, interval, open_time)
            )
            """)

    init_ema_table(str(db_path))

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(ema_indicators)")}

    assert {"ema12", "ema16", "ema21", "ema26"}.issubset(columns)


def test_run_loop_dispatches_interval_complete_after_all_interval_results(monkeypatch):
    import data_processor

    first = data_processor.MACalcResult(
        symbol="BTCUSDT",
        interval="15m",
        open_time=900_000,
        close_time=1_799_999,
        close=10.0,
        ma20=9.0,
        ema12=9.1,
        ema16=9.2,
        ema21=9.3,
        ema26=9.4,
        macd_dif=-0.3,
        macd_dea=-0.2,
        macd_histogram=-0.2,
    )
    second = data_processor.MACalcResult(
        symbol="ETHUSDT",
        interval="15m",
        open_time=900_000,
        close_time=1_799_999,
        close=20.0,
        ma20=19.0,
        ema12=19.1,
        ema16=19.2,
        ema21=19.3,
        ema26=19.4,
        macd_dif=-0.3,
        macd_dea=-0.2,
        macd_histogram=-0.2,
    )

    class FakeProcessor:
        def get_latest_ma20(self, symbol, interval):
            assert interval == "15m"
            return {"BTCUSDT": first, "ETHUSDT": second}[symbol]

    class FakeScheduler:
        def due_intervals(self):
            return ["15m"]

    events = []

    def on_result(result):
        events.append(("ema_saved", result.symbol))

    def on_interval_complete(interval, results):
        events.append(("macd_round", interval, [row.symbol for row in results]))

    def stop_after_one_loop(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(data_processor.time, "sleep", stop_after_one_loop)

    try:
        data_processor.run_loop(
            symbols=["BTCUSDT", "ETHUSDT"],
            processor=FakeProcessor(),
            scheduler=FakeScheduler(),
            on_result=on_result,
            on_interval_complete=on_interval_complete,
        )
    except KeyboardInterrupt:
        pass

    assert events == [
        ("ema_saved", "BTCUSDT"),
        ("ema_saved", "ETHUSDT"),
        ("macd_round", "15m", ["BTCUSDT", "ETHUSDT"]),
    ]


def test_run_loop_keeps_processing_when_interval_complete_callback_fails(monkeypatch):
    import data_processor

    first_round = data_processor.MACalcResult(
        symbol="BTCUSDT",
        interval="15m",
        open_time=900_000,
        close_time=1_799_999,
        close=10.0,
        ma20=9.0,
    )
    second_round = data_processor.MACalcResult(
        symbol="BTCUSDT",
        interval="15m",
        open_time=1_800_000,
        close_time=2_699_999,
        close=11.0,
        ma20=10.0,
    )

    class FakeProcessor:
        def __init__(self):
            self.calls = 0

        def get_latest_ma20(self, symbol, interval):
            assert symbol == "BTCUSDT"
            assert interval == "15m"
            self.calls += 1
            return first_round if self.calls == 1 else second_round

    class FakeScheduler:
        def due_intervals(self):
            return ["15m"]

    events = []
    sleep_calls = 0

    def on_result(result):
        events.append(("ma20_saved", result.open_time))

    def on_interval_complete(_interval, _results):
        events.append(("macd_callback", "failed"))
        raise RuntimeError("macd write failed")

    def stop_after_second_loop(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(data_processor.time, "sleep", stop_after_second_loop)

    try:
        data_processor.run_loop(
            symbols=["BTCUSDT"],
            processor=FakeProcessor(),
            scheduler=FakeScheduler(),
            on_result=on_result,
            on_interval_complete=on_interval_complete,
        )
    except KeyboardInterrupt:
        pass

    assert events == [
        ("ma20_saved", 900_000),
        ("macd_callback", "failed"),
        ("ma20_saved", 1_800_000),
        ("macd_callback", "failed"),
    ]
