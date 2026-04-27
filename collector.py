import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from binance.client import Client


# ================= 配置 =================
BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

BASE_INTERVAL = "1m"
AGG_INTERVALS = ["5m", "15m", "30m", "1h", "4h", "1d"]
ALL_INTERVALS = [BASE_INTERVAL, *AGG_INTERVALS]
LIMIT = 1000
MAX_WORKERS = 10

DATA_DIR = "data"
DB_PATH = f"{DATA_DIR}/klines.db"

is_running = False
lock = threading.Lock()
db_write_lock = threading.Lock()


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
                    PRIMARY KEY (symbol, open_time)
                )
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{tbl}_symbol_time "
                f"ON {tbl}(symbol, open_time)"
            )


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

    try:
        res = requests.get(BASE_URL, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()

        if isinstance(data, dict):
            print(
                f"⚠️ {symbol}: API error code={data.get('code')} msg={data.get('msg')}"
            )
            return []

        return data

    except Exception as e:
        print(f"⚠️ {symbol}: request failed: {e}")
        return []


def filter_closed_klines(klines):
    now_ms = int(time.time() * 1000)
    return [k for k in klines if int(k[6]) <= now_ms]


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


def upsert_aggregated_rows(symbol, interval, rows):
    if not rows:
        return 0

    tbl = table_name(interval)
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

    with db_write_lock:
        with get_db_conn() as conn:
            before = conn.total_changes
            conn.executemany(
                f"""
                INSERT INTO {tbl}
                (symbol, open_time, open, high, low, close, volume, close_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, open_time) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    close_time=excluded.close_time
                """,
                values,
            )
            return conn.total_changes - before


def aggregate_symbol(symbol, source_start_open_time, source_end_close_time):
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

            changed = upsert_aggregated_rows(symbol, interval, complete_target_rows)
            aggregated_stat[interval] = {
                "buckets": len(complete_target_rows),
                "changed": changed,
            }

    return aggregated_stat


# ================= 7. 单symbol处理 =================
def process_symbol(symbol):
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
        agg_stat = aggregate_symbol(
            symbol,
            source_start_open_time=int(all_klines[0][0]),
            source_end_close_time=int(all_klines[-1][6]),
        )

    return symbol, len(all_klines), inserted_count, agg_stat


# ================= 8. 主任务 =================
def main(universe):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_symbol, s) for s in universe]

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


# ================= 9. 定时任务 =================
def job():
    global is_running

    if is_running:
        print("⚠️ Skip (still running)")
        return

    with lock:
        is_running = True

    print("\n🟢 Job start:", time.strftime("%Y-%m-%d %H:%M:%S"))

    start = time.time()

    try:
        global UNIVERSE

        if UNIVERSE is None:
            UNIVERSE = build_universe()

        main(UNIVERSE)

    except Exception as e:
        print("❌ error:", e)

    print(f"⏱ cost: {round(time.time() - start, 2)}s")

    is_running = False


# ================= 全局 =================
UNIVERSE = None


# ================= 启动 =================
if __name__ == "__main__":
    init_db()

    scheduler = BlockingScheduler()

    scheduler.add_job(job, "cron", second=0)

    print(
        f"🚀 Alpha ∩ Futures Kline System started (source={BASE_INTERVAL}, aggregates={AGG_INTERVALS}, SQLite)"
    )
    print(f"🗄️ DB Path: {DB_PATH}")
    print("⏰ First run will start at the next full minute (second=00).")

    scheduler.start()
