"""Collect ALLUSDT 1h klines and persist MA20 values.

This module is intentionally independent from the main universe collector so the
ALLUSDT hourly indicator can be scheduled as a standalone data-collection task.
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
INTERVAL = "1h"
LIMIT = 1000
KLINE_TABLE = "allusdt_1h_klines"
MA20_TABLE = "allusdt_1h_ma20_indicators"
INTERVAL_MS = 60 * 60_000


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
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {KLINE_TABLE} (
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
        f"CREATE INDEX IF NOT EXISTS idx_{KLINE_TABLE}_open_time ON {KLINE_TABLE}(open_time)"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MA20_TABLE} (
            open_time INTEGER NOT NULL PRIMARY KEY,
            close_time INTEGER NOT NULL,
            close REAL NOT NULL,
            ma20 REAL NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{MA20_TABLE}_open_time ON {MA20_TABLE}(open_time)"
    )


def get_last_close_time(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute(
        f"SELECT close_time FROM {KLINE_TABLE} ORDER BY close_time DESC LIMIT 1"
    ).fetchone()
    return int(row[0]) if row else None


def get_latest_closed_close_time(now_ms: int) -> int:
    return (now_ms // INTERVAL_MS) * INTERVAL_MS - 1


def fetch_klines(start_time: Optional[int] = None, limit: int = LIMIT) -> list:
    params = {"symbol": SYMBOL, "interval": INTERVAL, "limit": limit}
    if start_time is not None:
        params["startTime"] = start_time

    for attempt in range(2):
        try:
            res = HTTP_SESSION.get(BASE_URL, params=params, timeout=(3, 10))
            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict):
                print(
                    f"⚠️ {SYMBOL} 1h API error "
                    f"code={data.get('code')} msg={data.get('msg')}"
                )
                return []
            return data
        except Exception as exc:
            if attempt == 0:
                time.sleep(0.2)
                continue
            print(f"⚠️ {SYMBOL} 1h request failed: {exc}")
            return []


def filter_closed_klines(klines: Iterable[list]) -> list:
    now_ms = int(time.time() * 1000)
    return [k for k in klines if int(k[6]) <= now_ms]


def save_klines(conn: sqlite3.Connection, klines: Iterable[list]) -> int:
    rows = [
        (
            int(k[0]),
            float(k[1]),
            float(k[2]),
            float(k[3]),
            float(k[4]),
            float(k[5]),
            int(k[6]),
        )
        for k in klines
    ]
    if not rows:
        return 0

    before = conn.total_changes
    conn.executemany(
        f"""
        INSERT OR IGNORE INTO {KLINE_TABLE}
        (open_time, open, high, low, close, volume, close_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return conn.total_changes - before


def save_ma20(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        f"""
        SELECT open_time, close_time, close
        FROM {KLINE_TABLE}
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
        values.append(
            (
                int(open_time),
                int(close_time),
                float(close),
                sum(closes[-20:]) / 20,
                now_ms,
            )
        )

    before = conn.total_changes
    conn.executemany(
        f"""
        INSERT INTO {MA20_TABLE}
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


def run(db_path: str, db_write_lock=None) -> tuple[int, int]:
    lock = db_write_lock
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000;")
        init_db(conn)
        last_close_time = get_last_close_time(conn)

    now_ms = int(time.time() * 1000)
    latest_closed_close_time = get_latest_closed_close_time(now_ms)
    if last_close_time is not None and last_close_time >= latest_closed_close_time:
        if lock is None:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                changed_ma20 = save_ma20(conn)
        else:
            with lock:
                with sqlite3.connect(db_path, timeout=30) as conn:
                    conn.execute("PRAGMA busy_timeout=30000;")
                    changed_ma20 = save_ma20(conn)
        print(f"✅ {SYMBOL} 1h up-to-date, ma20_changed={changed_ma20}")
        return 0, changed_ma20

    start_time = last_close_time + 1 if last_close_time is not None else None
    all_klines = []
    while True:
        klines = fetch_klines(start_time=start_time, limit=LIMIT)
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
            inserted = save_klines(conn, all_klines)
            ma20_changed = save_ma20(conn)
    else:
        with lock:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout=30000;")
                inserted = save_klines(conn, all_klines)
                ma20_changed = save_ma20(conn)

    print(
        f"✅ {SYMBOL} 1h fetched={len(all_klines)}, "
        f"inserted={inserted}, ma20_changed={ma20_changed}"
    )
    return inserted, ma20_changed
