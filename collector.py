import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from binance.client import Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ================= 配置 =================
BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"
BTC_5M_TABLE = "btc_usdt_5m_klines"
BTC_5M_INTERVAL = "5m"
BTC_5M_LIMIT = 1500

BASE_INTERVAL = "1m"
AGG_INTERVALS = ["5m", "15m", "30m", "1h", "4h", "1d"]
ALL_INTERVALS = [BASE_INTERVAL, *AGG_INTERVALS]
LIMIT = 1000
MAX_WORKERS = 10
OI_MAX_WORKERS = 4

DATA_DIR = os.getenv("DATA_DIR", "data")
DB_PATH = os.getenv("DB_PATH", f"{DATA_DIR}/klines.db")

kline_job_running = False
oi_job_running = False
funding_job_running = False
btc_5m_job_running = False
atr_15m_job_running = False
kline_job_lock = threading.Lock()
oi_job_lock = threading.Lock()
funding_job_lock = threading.Lock()
btc_5m_job_lock = threading.Lock()
atr_15m_job_lock = threading.Lock()
db_write_lock = threading.Lock()
last_funding_update_hour = None


def build_http_session(pool_size=MAX_WORKERS, total_retries=2):
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
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP_SESSION = build_http_session()


def table_name(interval):
    return f"klines_{interval}"


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# ================= SQLite 初始化 =================
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)

    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")

        init_btc_5m_table(conn)
        init_atr_15m_table(conn)

        for interval in ALL_INTERVALS:
            tbl = table_name(interval)
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    symbol TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    close_time INTEGER NOT NULL,
                    funding_rate REAL,
                    PRIMARY KEY (symbol, open_time)
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{tbl}_symbol_time "
                f"ON {tbl}(symbol, open_time)"
            )

        for funding_interval in ("15m", "1h"):
            tbl = table_name(funding_interval)
            columns = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
            column_names = {col[1] for col in columns}
            if "funding_rate" not in column_names:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN funding_rate REAL")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS open_interest_1m (
                symbol TEXT NOT NULL,
                snapshot_time INTEGER NOT NULL,
                open_interest REAL NOT NULL,
                PRIMARY KEY (symbol, snapshot_time)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_open_interest_1m_symbol_time "
            "ON open_interest_1m(symbol, snapshot_time)"
        )




def init_btc_5m_table(conn):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {BTC_5M_TABLE} (
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
        f"CREATE INDEX IF NOT EXISTS idx_{BTC_5M_TABLE}_open_time ON {BTC_5M_TABLE}(open_time)"
    )


def init_atr_15m_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS atr_15m_indicators (
            symbol TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            close_time INTEGER NOT NULL,
            atr14 REAL NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (symbol, open_time)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_atr_15m_symbol_time "
        "ON atr_15m_indicators(symbol, open_time)"
    )


def get_last_btc_5m_close_time():
    with get_db_conn() as conn:
        row = conn.execute(
            f"SELECT close_time FROM {BTC_5M_TABLE} ORDER BY close_time DESC LIMIT 1"
        ).fetchone()
    return int(row[0]) if row else None


def fetch_btc_5m_klines(start_time=None, limit=BTC_5M_LIMIT):
    params = {
        "symbol": "BTCUSDT",
        "interval": BTC_5M_INTERVAL,
        "limit": limit,
    }
    if start_time is not None:
        params["startTime"] = start_time

    for attempt in range(2):
        try:
            res = HTTP_SESSION.get(BASE_URL, params=params, timeout=(3, 10))
            res.raise_for_status()
            data = res.json()

            if isinstance(data, dict):
                print(f"⚠️ BTC 5m API error code={data.get('code')} msg={data.get('msg')}")
                return []
            return data
        except Exception as e:
            if attempt == 0:
                time.sleep(0.2)
                continue
            print(f"⚠️ BTC 5m request failed: {e}")
            return []


def save_btc_5m_klines(klines):
    if not klines:
        return 0

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

    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            conn.executemany(
                f"""
                INSERT OR IGNORE INTO {BTC_5M_TABLE}
                (open_time, open, high, low, close, volume, close_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return conn.total_changes - before


def run_btc_5m_main():
    interval_ms = interval_to_ms(BTC_5M_INTERVAL)
    now_ms = int(time.time() * 1000)
    latest_closed_close_time = get_latest_closed_close_time(now_ms, interval_ms)
    last_close_time = get_last_btc_5m_close_time()

    if last_close_time is not None and last_close_time >= latest_closed_close_time:
        print("✅ BTC 5m up-to-date")
        return

    start_time = last_close_time + 1 if last_close_time is not None else None
    all_klines = []

    while True:
        klines = fetch_btc_5m_klines(start_time=start_time, limit=BTC_5M_LIMIT)
        if not klines:
            break

        closed_klines = filter_closed_klines(klines)
        all_klines.extend(closed_klines)
        start_time = klines[-1][0] + 1

        if len(klines) < BTC_5M_LIMIT:
            break
        time.sleep(0.05)

    inserted = save_btc_5m_klines(all_klines)
    print(f"✅ BTC 5m fetched={len(all_klines)}, inserted={inserted}")

# ================= 1. U本位合约 =================
def get_um_symbols():
    client = Client()
    info = client.futures_exchange_info()

    return {
        s["symbol"].replace("USDT", "")
        for s in info["symbols"]
        if s["status"] == "TRADING" and s["symbol"].endswith("USDT")
    }


# ================= 2. Alpha tokens =================
def get_alpha_symbols():
    url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=10).json()

    tokens = res.get("data", [])
    if isinstance(tokens, dict):
        tokens = tokens.get("list") or tokens.get("tokens") or []

    return {t["symbol"].upper() for t in tokens if t.get("symbol")}


# ================= 3. 构建 Universe（关键） =================
def build_universe():
    um = get_um_symbols()
    alpha = get_alpha_symbols()

    universe = sorted(um & alpha)

    print(f"UM symbols: {len(um)}")
    print(f"Alpha symbols: {len(alpha)}")
    print(f"Universe (intersection): {len(universe)}")

    return universe


# ================= 4. 获取SQLite最后时间 =================
def get_last_kline_record(symbol, interval):
    tbl = table_name(interval)
    with get_db_conn() as conn:
        row = conn.execute(
            f"""
            SELECT open_time, close_time
            FROM {tbl}
            WHERE symbol = ?
            ORDER BY close_time DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()

    if not row:
        return None, None

    return int(row[0]), int(row[1])


def interval_to_ms(interval):
    matched = re.fullmatch(r"(\d+)([mhdw])", interval)
    if not matched:
        raise ValueError(f"Unsupported interval: {interval}")

    value = int(matched.group(1))
    unit = matched.group(2)
    unit_ms = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[unit]
    return value * unit_ms


def get_latest_closed_close_time(now_ms, interval_ms):
    return (now_ms // interval_ms) * interval_ms - 1


# ================= 5. K线请求 =================
def fetch_klines(symbol, start_time=None, limit=LIMIT):
    params = {
        "symbol": f"{symbol}USDT",
        "interval": BASE_INTERVAL,
        "limit": limit,
    }

    if start_time:
        params["startTime"] = start_time

    for attempt in range(2):
        try:
            res = HTTP_SESSION.get(BASE_URL, params=params, timeout=(3, 10))
            res.raise_for_status()
            data = res.json()

            if isinstance(data, dict):
                print(
                    f"⚠️ {symbol}: API error code={data.get('code')} msg={data.get('msg')}"
                )
                return []

            return data

        except Exception as e:
            if attempt == 0:
                time.sleep(0.2)
                continue
            print(f"⚠️ {symbol}: request failed: {e}")
            return []


def filter_closed_klines(klines):
    now_ms = int(time.time() * 1000)
    return [k for k in klines if int(k[6]) <= now_ms]


def fetch_all_funding_rates():
    try:
        res = HTTP_SESSION.get(FUNDING_RATE_URL, timeout=(3, 10))
        res.raise_for_status()
        data = res.json()
        if not isinstance(data, list):
            return {}

        rates = {}
        for row in data:
            symbol = row.get("symbol")
            funding_rate = row.get("lastFundingRate")
            if not symbol or funding_rate is None:
                continue
            try:
                rates[symbol] = float(funding_rate)
            except (TypeError, ValueError):
                continue
        return rates
    except Exception as e:
        print(f"⚠️ funding rate request failed: {e}")
        return {}


def fetch_open_interest(symbol):
    for attempt in range(4):
        try:
            res = HTTP_SESSION.get(
                OPEN_INTEREST_URL,
                params={"symbol": f"{symbol}USDT"},
                timeout=(3, 15),
            )
            res.raise_for_status()
            data = res.json()
            if not isinstance(data, dict):
                return None
            open_interest = data.get("openInterest")
            if open_interest is None:
                return None
            return float(open_interest)
        except Exception as e:
            if attempt < 3:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"⚠️ {symbol}: open interest request failed: {e}")
            return None


def save_open_interest(symbol, open_interest):
    if open_interest is None:
        return 0

    now_ms = int(time.time() * 1000)
    snapshot_time = (now_ms // 60_000) * 60_000

    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO open_interest_1m
                (symbol, snapshot_time, open_interest)
                VALUES (?, ?, ?)
                """,
                (symbol, snapshot_time, open_interest),
            )
            return conn.total_changes - before


FUNDING_INTERVALS = ("1h",)
REALTIME_15M_FUNDING_BACKFILL_BARS = 12


def save_funding_rates_to_interval(
    universe, funding_rates, interval, target_open_times, only_missing=False
):
    if not universe or not funding_rates or not target_open_times:
        return 0

    values = []
    for symbol in universe:
        usdt_symbol = f"{symbol}USDT"
        if usdt_symbol not in funding_rates:
            continue
        for target_open_time in target_open_times:
            values.append((funding_rates[usdt_symbol], symbol, target_open_time))

    if not values:
        return 0

    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            missing_clause = " AND funding_rate IS NULL" if only_missing else ""
            conn.executemany(
                f"""
                UPDATE {table_name(interval)}
                SET funding_rate = ?
                WHERE symbol = ? AND open_time = ?{missing_clause}
                """,
                values,
            )
            return conn.total_changes - before


def save_funding_rates_to_1h(universe, funding_rates, target_open_times):
    return save_funding_rates_to_interval(
        universe, funding_rates, "1h", target_open_times
    )


def save_realtime_15m_funding_rates(universe, funding_rates):
    target_open_times = get_recent_target_open_times(
        "15m", count=REALTIME_15M_FUNDING_BACKFILL_BARS
    )
    return save_funding_rates_to_interval(
        universe, funding_rates, "15m", target_open_times, only_missing=True
    )


def get_recent_target_open_times(interval="1h", count=None, hours=None):
    interval_ms = interval_to_ms(interval)
    now_ms = int(time.time() * 1000)
    current_interval_open = (now_ms // interval_ms) * interval_ms

    if count is None:
        count = hours if hours is not None else 3

    return [current_interval_open - interval_ms * i for i in range(1, count + 1)]


def get_funding_backfill_counts(backfill_hours):
    return {
        interval: max(1, backfill_hours * interval_to_ms("1h") // interval_to_ms(interval))
        for interval in FUNDING_INTERVALS
    }


def should_run_funding_update(now_ms):
    global last_funding_update_hour

    hour_ms = interval_to_ms("1h")
    current_hour_open = (now_ms // hour_ms) * hour_ms

    if last_funding_update_hour is None:
        return True

    return (current_hour_open - last_funding_update_hour) >= hour_ms


def run_funding_update(universe, backfill_hours=3, backfill_counts=None):
    global last_funding_update_hour

    now_ms = int(time.time() * 1000)
    if not should_run_funding_update(now_ms):
        return 0

    funding_rates = fetch_all_funding_rates()
    if not funding_rates:
        print("⚠️ funding rates empty, skip update")
        return 0

    counts = backfill_counts or get_funding_backfill_counts(backfill_hours)
    updated_by_interval = {}
    for interval, count in counts.items():
        target_open_times = get_recent_target_open_times(interval, count=count)
        updated = save_funding_rates_to_interval(
            universe, funding_rates, interval, target_open_times
        )
        updated_by_interval[interval] = {
            "updated": updated,
            "targets": len(target_open_times),
        }

    hour_ms = interval_to_ms("1h")
    last_funding_update_hour = (now_ms // hour_ms) * hour_ms
    summary = ", ".join(
        f"{interval}: rows={stat['updated']}, targets={stat['targets']}"
        for interval, stat in updated_by_interval.items()
    )
    total_updated = sum(stat["updated"] for stat in updated_by_interval.values())
    print(f"💰 funding_rate updated ({summary})")
    return total_updated


# ================= 6. 写SQLite =================
def save_to_sqlite(symbol, klines, interval):
    tbl = table_name(interval)
    rows = [
        (
            symbol,
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

    for attempt in range(5):
        try:
            with db_write_lock:
                with get_db_conn() as conn:
                    before = conn.total_changes
                    conn.executemany(
                        f"""
                        INSERT OR IGNORE INTO {tbl}
                        (symbol, open_time, open, high, low, close, volume, close_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                    inserted = conn.total_changes - before
            return inserted
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < 4:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise


def upsert_aggregated_rows(symbol, interval, rows, funding_rate=None):
    if not rows:
        return 0

    tbl = table_name(interval)
    include_funding_rate = interval == "15m" and funding_rate is not None

    if include_funding_rate:
        values = [
            (
                symbol,
                r["open_time"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                r["close_time"],
                funding_rate,
            )
            for r in rows
        ]
        insert_columns = (
            "symbol, open_time, open, high, low, close, volume, close_time, funding_rate"
        )
        placeholders = "?, ?, ?, ?, ?, ?, ?, ?, ?"
        funding_update = ",\n                    funding_rate=excluded.funding_rate"
    else:
        values = [
            (
                symbol,
                r["open_time"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                r["close_time"],
            )
            for r in rows
        ]
        insert_columns = "symbol, open_time, open, high, low, close, volume, close_time"
        placeholders = "?, ?, ?, ?, ?, ?, ?, ?"
        funding_update = ""

    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            conn.executemany(
                f"""
                INSERT INTO {tbl}
                ({insert_columns})
                VALUES ({placeholders})
                ON CONFLICT(symbol, open_time) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    close_time=excluded.close_time{funding_update}
                """,
                values,
            )
            return conn.total_changes - before


def aggregate_symbol(
    symbol, source_start_open_time, source_end_close_time, funding_rate=None
):
    source_interval_ms = interval_to_ms(BASE_INTERVAL)

    with get_db_conn() as conn:
        aggregated_stat = {}

        for interval in AGG_INTERVALS:
            target_ms = interval_to_ms(interval)
            bars_per_target = target_ms // source_interval_ms

            range_start = (source_start_open_time // target_ms) * target_ms
            range_end_open = (source_end_close_time // target_ms) * target_ms
            range_end_exclusive = range_end_open + target_ms

            source_rows = conn.execute(
                f"""
                SELECT open_time, open, high, low, close, volume
                FROM {table_name(BASE_INTERVAL)}
                WHERE symbol = ?
                  AND open_time >= ?
                  AND open_time < ?
                ORDER BY open_time ASC
                """,
                (symbol, range_start, range_end_exclusive),
            ).fetchall()

            buckets = {}
            for open_time, op, hi, lo, cl, vol in source_rows:
                bucket = (open_time // target_ms) * target_ms
                buckets.setdefault(bucket, []).append(
                    {
                        "open_time": int(open_time),
                        "open": float(op),
                        "high": float(hi),
                        "low": float(lo),
                        "close": float(cl),
                        "volume": float(vol),
                    }
                )

            complete_target_rows = []
            for bucket_open, bars in sorted(buckets.items()):
                if len(bars) != bars_per_target:
                    continue

                expected_first_open = bucket_open
                expected_last_open = bucket_open + target_ms - source_interval_ms
                if (
                    bars[0]["open_time"] != expected_first_open
                    or bars[-1]["open_time"] != expected_last_open
                ):
                    continue

                complete_target_rows.append(
                    {
                        "open_time": bucket_open,
                        "open": bars[0]["open"],
                        "high": max(b["high"] for b in bars),
                        "low": min(b["low"] for b in bars),
                        "close": bars[-1]["close"],
                        "volume": sum(b["volume"] for b in bars),
                        "close_time": bucket_open + target_ms - 1,
                    }
                )

            changed = upsert_aggregated_rows(
                symbol, interval, complete_target_rows, funding_rate=funding_rate
            )
            aggregated_stat[interval] = {
                "buckets": len(complete_target_rows),
                "changed": changed,
            }

    return aggregated_stat


# ================= 7. 单symbol处理 =================
def process_symbol_kline(symbol, funding_rates=None):
    interval_ms = interval_to_ms(BASE_INTERVAL)
    now_ms = int(time.time() * 1000)
    latest_closed_close_time = get_latest_closed_close_time(now_ms, interval_ms)
    _, last_close_time = get_last_kline_record(symbol, BASE_INTERVAL)

    if last_close_time is not None and last_close_time >= latest_closed_close_time:
        return symbol, 0, 0, {}

    start_time = last_close_time + 1 if last_close_time is not None else None
    missing_klines = None
    if last_close_time is not None:
        missing_klines = (latest_closed_close_time - last_close_time) // interval_ms

    all_klines = []

    while True:
        request_limit = LIMIT
        if missing_klines is not None:
            remaining = missing_klines - len(all_klines)
            if remaining <= 0:
                break
            request_limit = min(LIMIT, remaining)

        klines = fetch_klines(symbol, start_time, limit=request_limit)

        if not klines:
            break

        closed_klines = filter_closed_klines(klines)
        if missing_klines is not None:
            remaining = missing_klines - len(all_klines)
            closed_klines = closed_klines[:remaining]

        all_klines.extend(closed_klines)

        start_time = klines[-1][0] + 1

        if len(klines) < request_limit:
            break

        time.sleep(0.05)

    inserted_count = 0
    agg_stat = {}
    if all_klines:
        inserted_count = save_to_sqlite(symbol, all_klines, BASE_INTERVAL)
        funding_rate = None
        if funding_rates:
            funding_rate = funding_rates.get(f"{symbol}USDT")
        agg_stat = aggregate_symbol(
            symbol,
            source_start_open_time=int(all_klines[0][0]),
            source_end_close_time=int(all_klines[-1][6]),
            funding_rate=funding_rate,
        )

    return symbol, len(all_klines), inserted_count, agg_stat


def process_symbol_oi(symbol):
    open_interest = fetch_open_interest(symbol)
    oi_inserted = save_open_interest(symbol, open_interest)
    return symbol, oi_inserted


def calculate_atr14_from_15m_rows(rows):
    """Calculate ATR(14) from 15m kline rows using the latest 14 true ranges."""
    if len(rows) < 15:
        return None

    true_ranges = []
    previous_close = float(rows[0][3])
    for _open_time, high, low, close, _close_time in rows[1:]:
        high = float(high)
        low = float(low)
        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )
        previous_close = float(close)

    return sum(true_ranges[-14:]) / 14


def process_symbol_atr_15m(symbol):
    with get_db_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT open_time, high, low, close, close_time
            FROM {table_name("15m")}
            WHERE symbol = ?
            ORDER BY open_time DESC
            LIMIT 15
            """,
            (symbol,),
        ).fetchall()

    if len(rows) < 15:
        return symbol, 0, None

    rows = list(reversed(rows))
    latest_open_time = int(rows[-1][0])
    latest_close_time = int(rows[-1][4])
    expected_close_time = latest_open_time + interval_to_ms("15m") - 1
    if latest_close_time != expected_close_time:
        return symbol, 0, None

    atr14 = calculate_atr14_from_15m_rows(rows)
    if atr14 is None:
        return symbol, 0, None

    now_ms = int(time.time() * 1000)
    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            conn.execute(
                """
                INSERT INTO atr_15m_indicators
                (symbol, open_time, close_time, atr14, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, open_time) DO UPDATE SET
                    close_time=excluded.close_time,
                    atr14=excluded.atr14,
                    updated_at=excluded.updated_at
                """,
                (symbol, latest_open_time, latest_close_time, atr14, now_ms),
            )
            changed = conn.total_changes - before

    return symbol, changed, atr14


# ================= 8. 主任务 =================
def run_kline_main(universe):
    funding_rates = fetch_all_funding_rates()
    if not funding_rates:
        print("⚠️ realtime 15m funding rates empty, 15m klines keep existing funding_rate")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_symbol_kline, s, funding_rates) for s in universe
        ]

        for future in as_completed(futures):
            try:
                symbol, fetched_count, inserted_count, agg_stat = future.result()
                agg_summary = ", ".join(
                    [
                        f"{k}:buckets={v['buckets']},changed={v['changed']}"
                        for k, v in agg_stat.items()
                    ]
                )
                if not agg_summary:
                    agg_summary = "-"
                print(
                    f"✅ {symbol}: fetched={fetched_count}, inserted_1m={inserted_count}, agg=({agg_summary})"
                )
            except Exception as e:
                print(f"❌ symbol task failed: {e}")

    if funding_rates:
        updated_15m = save_realtime_15m_funding_rates(universe, funding_rates)
        print(f"💰 realtime 15m funding_rate updated rows={updated_15m}")


def run_oi_main(universe):
    with ThreadPoolExecutor(max_workers=OI_MAX_WORKERS) as executor:
        futures = [executor.submit(process_symbol_oi, s) for s in universe]

        for future in as_completed(futures):
            try:
                symbol, oi_inserted = future.result()
                print(f"✅ {symbol}: inserted_oi_1m={oi_inserted}")
            except Exception as e:
                print(f"❌ oi symbol task failed: {e}")


def run_atr_15m_main(universe):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_symbol_atr_15m, s) for s in universe]

        for future in as_completed(futures):
            try:
                symbol, changed, atr14 = future.result()
                if atr14 is None:
                    print(f"⏭️ {symbol}: atr14_15m skipped")
                    continue
                print(f"✅ {symbol}: atr14_15m={atr14}, changed={changed}")
            except Exception as e:
                print(f"❌ atr 15m symbol task failed: {e}")


# ================= 9. 定时任务 =================
def kline_job():
    global kline_job_running

    if kline_job_running:
        print("⚠️ Skip kline job (still running)")
        return

    with kline_job_lock:
        kline_job_running = True

    print("\n🟢 Kline job start:", time.strftime("%Y-%m-%d %H:%M:%S"))

    start = time.time()

    try:
        global UNIVERSE

        if UNIVERSE is None:
            UNIVERSE = build_universe()

        run_kline_main(UNIVERSE)

    except Exception as e:
        print("❌ error:", e)

    print(f"⏱ cost: {round(time.time() - start, 2)}s")

    kline_job_running = False


def oi_job():
    global oi_job_running

    if oi_job_running:
        print("⚠️ Skip oi job (still running)")
        return

    with oi_job_lock:
        oi_job_running = True

    print("\n🔵 OI job start:", time.strftime("%Y-%m-%d %H:%M:%S"))

    start = time.time()

    try:
        global UNIVERSE

        if UNIVERSE is None:
            UNIVERSE = build_universe()

        run_oi_main(UNIVERSE)
    except Exception as e:
        print("❌ oi error:", e)

    print(f"⏱ oi cost: {round(time.time() - start, 2)}s")

    oi_job_running = False


def funding_job():
    global funding_job_running

    if funding_job_running:
        print("⚠️ Skip funding job (still running)")
        return

    with funding_job_lock:
        funding_job_running = True

    print("\n🟣 Funding job start:", time.strftime("%Y-%m-%d %H:%M:%S"))

    start = time.time()

    try:
        global UNIVERSE

        if UNIVERSE is None:
            UNIVERSE = build_universe()

        run_funding_update(UNIVERSE)
    except Exception as e:
        print("❌ funding error:", e)

    print(f"⏱ funding cost: {round(time.time() - start, 2)}s")

    funding_job_running = False




def btc_5m_job():
    global btc_5m_job_running

    if btc_5m_job_running:
        print("⚠️ Skip BTC 5m job (still running)")
        return

    with btc_5m_job_lock:
        btc_5m_job_running = True

    print("\n🟡 BTC 5m job start:", time.strftime("%Y-%m-%d %H:%M:%S"))
    start = time.time()

    try:
        run_btc_5m_main()
    except Exception as e:
        print("❌ BTC 5m error:", e)

    print(f"⏱ BTC 5m cost: {round(time.time() - start, 2)}s")

    btc_5m_job_running = False


def atr_15m_job():
    global atr_15m_job_running

    if atr_15m_job_running:
        print("⚠️ Skip ATR 15m job (still running)")
        return

    with atr_15m_job_lock:
        atr_15m_job_running = True

    print("\n🟠 ATR 15m job start:", time.strftime("%Y-%m-%d %H:%M:%S"))
    start = time.time()

    try:
        global UNIVERSE

        if UNIVERSE is None:
            UNIVERSE = build_universe()

        run_atr_15m_main(UNIVERSE)
    except Exception as e:
        print("❌ ATR 15m error:", e)

    print(f"⏱ ATR 15m cost: {round(time.time() - start, 2)}s")

    atr_15m_job_running = False

# ================= 全局 =================
UNIVERSE = None


# ================= 启动 =================
if __name__ == "__main__":
    init_db()

    scheduler = BlockingScheduler()

    scheduler.add_job(kline_job, "cron", second=0)
    scheduler.add_job(oi_job, "cron", second=20)
    scheduler.add_job(funding_job, "cron", minute=1, second=40)
    scheduler.add_job(btc_5m_job, "cron", minute="*/5", second=10)
    scheduler.add_job(atr_15m_job, "cron", minute="*/15", second=30)

    print(
        f"🚀 Alpha ∩ Futures Kline System started (source={BASE_INTERVAL}, aggregates={AGG_INTERVALS}, SQLite)"
    )
    print(f"🗄️ DB Path: {DB_PATH}")
    print("⏰ First run will start at the next full minute (second=00).")

    scheduler.start()
