"""Hourly Binance real-time funding-rate collector for alpha.db.

Runs at HH:01:00 UTC by default and stores only records whose interval is 1h.
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = os.getenv(
    "BINANCE_REALTIME_FUNDING_URL",
    "https://www.binance.com/bapi/futures/v1/public/future/fundingRate/realTimeFundingRate",
)
DB_PATH = os.getenv("ALPHA_DB_PATH", "data/alpha.db")
LOG = logging.getLogger("alpha_funding_collector")


def http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, connect=3, read=3, status=3, backoff_factor=0.5,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset({"GET"}))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _rows(payload: Any) -> Iterable[dict[str, Any]]:
    """Accept Binance's list or wrapped {data: ...} response variants."""
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(data, dict):
        data = data.get("list", data.get("rows", []))
    return data if isinstance(data, list) else []


def fetch_funding_rates(session: requests.Session | None = None) -> list[dict[str, Any]]:
    session = session or http_session()
    response = session.get(API_URL, timeout=20, headers={"User-Agent": "alpha-funding-collector/1.0"})
    response.raise_for_status()
    payload = response.json()
    result = []
    for item in _rows(payload):
        if not isinstance(item, dict):
            continue
        interval = str(item.get("interval", item.get("fundingInterval", ""))).lower()
        if interval not in {"1h", "1hour", "60m", "60min"}:
            continue
        symbol = item.get("symbol")
        rate = item.get("fundingRate", item.get("funding_rate"))
        if symbol and rate is not None:
            result.append({"symbol": str(symbol), "funding_rate": float(rate), "raw": item})
    return result


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS alpha_realtime_funding (
        symbol TEXT NOT NULL,
        observed_at INTEGER NOT NULL,
        funding_rate REAL NOT NULL,
        interval TEXT NOT NULL DEFAULT '1h',
        payload_json TEXT,
        PRIMARY KEY(symbol, observed_at)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alpha_realtime_funding_time ON alpha_realtime_funding(observed_at)")


def save_rates(rates: Iterable[dict[str, Any]], db_path: str = DB_PATH, observed_at: int | None = None) -> int:
    import json
    observed_at = int(observed_at if observed_at is not None else time.time() * 1000)
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    with sqlite3.connect(db_path, timeout=30) as conn:
        init_db(conn)
        values = [(r["symbol"], observed_at, r["funding_rate"], "1h", json.dumps(r.get("raw", {}), ensure_ascii=False)) for r in rates]
        conn.executemany("INSERT OR REPLACE INTO alpha_realtime_funding(symbol, observed_at, funding_rate, interval, payload_json) VALUES (?, ?, ?, ?, ?)", values)
        return len(values)


def collect_once(db_path: str = DB_PATH, session: requests.Session | None = None) -> int:
    rates = fetch_funding_rates(session)
    count = save_rates(rates, db_path)
    LOG.info("saved %d 1h funding-rate symbols", count)
    return count


def seconds_until_next_run(now: float | None = None) -> float:
    now = time.time() if now is None else now
    current = datetime.fromtimestamp(now, timezone.utc)
    target = current.replace(minute=1, second=0, microsecond=0)
    if target.timestamp() <= now:
        target = target + timedelta(hours=1)
    return max(0.0, target.timestamp() - now)


def run_forever(db_path: str = DB_PATH) -> None:
    while True:
        time.sleep(seconds_until_next_run())
        try:
            collect_once(db_path)
        except Exception:
            LOG.exception("hourly funding collection failed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="collect immediately")
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.once:
        collect_once(args.db)
    else:
        run_forever(args.db)
