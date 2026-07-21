"""Collect ALLUSDT 15m klines and persist MA20 values.

This module is intentionally independent from the main universe collector so the
ALLUSDT 15-minute indicator can be scheduled as a standalone data-collection task.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
SYMBOL = "ALLUSDT"
INTERVAL = "15m"
LIMIT = 1000
KLINE_TABLE = "allusdt_15m_klines"
MA20_TABLE = "allusdt_15m_ma20_indicators"
INTERVAL_MS = 15 * 60_000

H1_INTERVAL = "1h"
H1_KLINE_TABLE = "allusdt_1h_klines"
H1_MA20_TABLE = "allusdt_1h_ma20_indicators"
H1_INTERVAL_MS = 60 * 60_000

INTERVAL_CONFIGS = (
    (INTERVAL, KLINE_TABLE, MA20_TABLE, INTERVAL_MS),
    (H1_INTERVAL, H1_KLINE_TABLE, H1_MA20_TABLE, H1_INTERVAL_MS),
)


def build_http_session(total_retries: int = 2) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP_SESSION = build_http_session()


def init_db(conn: sqlite3.Connection) -> None:
    for _interval, kline_table, ma20_table, _interval_ms in INTERVAL_CONFIGS:
        init_interval_db(conn, kline_table, ma20_table)


def init_interval_db(conn: sqlite3.Connection, kline_table: str, ma20_table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {kline_table} (
            open_time INTEGER NOT NULL PRIMARY KEY,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            close_time INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{kline_table}_open_time ON {kline_table}(open_time)"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ma20_table} (
            open_time INTEGER NOT NULL PRIMARY KEY,
            close_time INTEGER NOT NULL,
            close REAL NOT NULL,
            ma20 REAL NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{ma20_table}_open_time ON {ma20_table}(open_time)"
    )


def get_last_close_time(conn: sqlite3.Connection, kline_table: str = KLINE_TABLE) -> Optional[int]:
    row = conn.execute(
        f"SELECT close_time FROM {kline_table} ORDER BY close_time DESC LIMIT 1"
    ).fetchone()
    return int(row[0]) if row else None


def get_latest_closed_close_time(now_ms: int, interval_ms: int = INTERVAL_MS) -> int:
    return (now_ms // interval_ms) * interval_ms - 1


def fetch_klines(
    start_time: Optional[int] = None,
    limit: int = LIMIT,
    interval: str = INTERVAL,
) -> list:
    params = {"symbol": SYMBOL, "interval": interval, "limit": limit}
    if start_time is not None:
        params["startTime"] = start_time

    for attempt in range(2):
        try:
            res = HTTP_SESSION.get(BASE_URL, params=params, timeout=(3, 10))
            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict):
                print(
                    f"⚠️ {SYMBOL} {interval} API error "
                    f"code={data.get('code')} msg={data.get('msg')}"
                )
                return []
            return data
        except Exception as exc:
            if attempt == 0:
                time.sleep(0.2)
                continue
            print(f"⚠️ {SYMBOL} {interval} request failed: {exc}")
            return []


def filter_closed_klines(klines: Iterable[list]) -> list:
    now_ms = int(time.time() * 1000)
    return [k for k in klines if int(k[6]) <= now_ms]


def save_klines(
    conn: sqlite3.Connection,
    klines: Iterable[list],
    kline_table: str = KLINE_TABLE,
) -> int:
    rows = [
        (int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), int(k[6]))
        for k in klines
    ]
    if not rows:
        return 0

    before = conn.total_changes
    conn.executemany(
        f"""
        INSERT OR IGNORE INTO {kline_table}
        (open_time, open, high, low, close, volume, close_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return conn.total_changes - before


def save_ma20(
    conn: sqlite3.Connection,
    kline_table: str = KLINE_TABLE,
    ma20_table: str = MA20_TABLE,
) -> int:
    rows = conn.execute(
        f"""
        SELECT open_time, close_time, close
        FROM {kline_table}
        ORDER BY open_time ASC
        """
    ).fetchall()
    if len(rows) < 20:
        return 0

    now_ms = int(time.time() * 1000)
    values = []
    closes = []
    for open_time, close_time, close in rows:
        closes.append(float(close))
        if len(closes) < 20:
            continue
        values.append((int(open_time), int(close_time), float(close), sum(closes[-20:]) / 20, now_ms))

    before = conn.total_changes
    conn.executemany(
        f"""
        INSERT INTO {ma20_table}
        (open_time, close_time, close, ma20, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(open_time) DO UPDATE SET
            close_time=excluded.close_time,
            close=excluded.close,
            ma20=excluded.ma20,
            updated_at=excluded.updated_at
        """,
        values,
    )
    return conn.total_changes - before


def run_interval(
    db_path: str,
    interval: str,
    kline_table: str,
    ma20_table: str,
    interval_ms: int,
    db_write_lock=None,
) -> tuple[int, int]:
    lock = db_write_lock
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000;")
        init_interval_db(conn, kline_table, ma20_table)
        last_close_time = get_last_close_time(conn, kline_table)

    now_ms = int(time.time() * 1000)
    latest_closed_close_time = get_latest_closed_close_time(now_ms, interval_ms)
    if last_close_time is not None and last_close_time >= latest_closed_close_time:
        if lock is None:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                changed_ma20 = save_ma20(conn, kline_table, ma20_table)
        else:
            with lock:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    conn.execute("PRAGMA busy_timeout=30000;")
                    changed_ma20 = save_ma20(conn, kline_table, ma20_table)
        print(f"✅ {SYMBOL} {interval} up-to-date, ma20_changed={changed_ma20}")
        return 0, changed_ma20

    start_time = last_close_time + 1 if last_close_time is not None else None
    all_klines = []
    while True:
        klines = fetch_klines(start_time=start_time, limit=LIMIT, interval=interval)
        if not klines:
            break
        all_klines.extend(filter_closed_klines(klines))
        start_time = int(klines[-1][0]) + 1
        if len(klines) < LIMIT:
            break
        time.sleep(0.05)

    if lock is None:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA busy_timeout=30000;")
            inserted = save_klines(conn, all_klines, kline_table)
            ma20_changed = save_ma20(conn, kline_table, ma20_table)
    else:
        with lock:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                inserted = save_klines(conn, all_klines, kline_table)
                ma20_changed = save_ma20(conn, kline_table, ma20_table)

    print(f"✅ {SYMBOL} {interval} fetched={len(all_klines)}, inserted={inserted}, ma20_changed={ma20_changed}")
    return inserted, ma20_changed


def run(db_path: str, db_write_lock=None) -> tuple[int, int]:
    total_inserted = 0
    total_ma20_changed = 0
    for interval, kline_table, ma20_table, interval_ms in INTERVAL_CONFIGS:
        inserted, ma20_changed = run_interval(
            db_path,
            interval,
            kline_table,
            ma20_table,
            interval_ms,
            db_write_lock=db_write_lock,
        )
        total_inserted += inserted
        total_ma20_changed += ma20_changed
    return total_inserted, total_ma20_changed


def run_recent_backfill_interval(
    db_path: str,
    interval: str,
    kline_table: str,
    ma20_table: str,
    interval_ms: int,
    hours: int = 4,
    db_write_lock=None,
) -> tuple[int, int]:
    lock = db_write_lock
    now_ms = int(time.time() * 1000)
    latest_closed_close_time = get_latest_closed_close_time(now_ms, interval_ms)
    earliest_open_time = ((now_ms - hours * 60 * 60_000) // interval_ms) * interval_ms

    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000;")
        init_interval_db(conn, kline_table, ma20_table)
        existing_open_times = {
            int(row[0])
            for row in conn.execute(
                f"""
                SELECT open_time
                FROM {kline_table}
                WHERE open_time >= ? AND close_time <= ?
                """,
                (earliest_open_time, latest_closed_close_time),
            ).fetchall()
        }

    expected_open_times = set(range(earliest_open_time, latest_closed_close_time + 1, interval_ms))
    missing_open_times = expected_open_times - existing_open_times
    if not missing_open_times:
        if lock is None:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                ma20_changed = save_ma20(conn, kline_table, ma20_table)
        else:
            with lock:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    conn.execute("PRAGMA busy_timeout=30000;")
                    ma20_changed = save_ma20(conn, kline_table, ma20_table)
        print(f"✅ {SYMBOL} {interval} recent {hours}h backfill up-to-date, ma20_changed={ma20_changed}")
        return 0, ma20_changed

    klines = fetch_klines(start_time=min(missing_open_times), limit=len(expected_open_times) + 2, interval=interval)
    target_klines = [
        k for k in filter_closed_klines(klines)
        if earliest_open_time <= int(k[0]) <= latest_closed_close_time
    ]

    if lock is None:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA busy_timeout=30000;")
            inserted = save_klines(conn, target_klines, kline_table)
            ma20_changed = save_ma20(conn, kline_table, ma20_table)
    else:
        with lock:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                inserted = save_klines(conn, target_klines, kline_table)
                ma20_changed = save_ma20(conn, kline_table, ma20_table)

    print(
        f"✅ {SYMBOL} {interval} recent {hours}h backfill "
        f"missing={len(missing_open_times)}, fetched={len(target_klines)}, "
        f"inserted={inserted}, ma20_changed={ma20_changed}"
    )
    return inserted, ma20_changed


def run_recent_backfill(db_path: str, hours: int = 4, db_write_lock=None) -> tuple[int, int]:
    total_inserted = 0
    total_ma20_changed = 0
    for interval, kline_table, ma20_table, interval_ms in INTERVAL_CONFIGS:
        inserted, ma20_changed = run_recent_backfill_interval(
            db_path,
            interval,
            kline_table,
            ma20_table,
            interval_ms,
            hours=hours,
            db_write_lock=db_write_lock,
        )
        total_inserted += inserted
        total_ma20_changed += ma20_changed
    return total_inserted, total_ma20_changed
