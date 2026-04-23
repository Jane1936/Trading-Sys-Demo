import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from binance.client import Client


# ================= 配置 =================
BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

INTERVAL = "1m"
LIMIT = 1000
MAX_WORKERS = 10

DATA_DIR = "data"
DB_PATH = f"{DATA_DIR}/klines_{INTERVAL}.db"
TABLE_NAME = f"klines_{INTERVAL}"

is_running = False
lock = threading.Lock()


# ================= SQLite 初始化 =================
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)

    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_symbol_time "
            f"ON {TABLE_NAME}(symbol, open_time)"
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
def get_last_timestamp(symbol):
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        row = conn.execute(
            f"SELECT MAX(open_time) FROM {TABLE_NAME} WHERE symbol = ?", (symbol,)
        ).fetchone()

    return row[0] if row and row[0] is not None else None


# ================= 5. K线请求 =================
def fetch_klines(symbol, start_time=None):
    params = {
        "symbol": f"{symbol}USDT",
        "interval": INTERVAL,
        "limit": LIMIT,
    }

    if start_time:
        params["startTime"] = start_time

    try:
        res = requests.get(BASE_URL, params=params, timeout=10)
        data = res.json()

        if isinstance(data, dict):
            return []

        return data

    except Exception:
        return []


# ================= 6. 写SQLite =================
def save_to_sqlite(symbol, klines):
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

    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.executemany(
            f"""
            INSERT OR IGNORE INTO {TABLE_NAME}
            (symbol, open_time, open, high, low, close, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


# ================= 7. 单symbol处理 =================
def process_symbol(symbol):
    last_ts = get_last_timestamp(symbol)
    start_time = last_ts + 1 if last_ts else None

    all_klines = []

    while True:
        klines = fetch_klines(symbol, start_time)

        if not klines:
            break

        all_klines.extend(klines)

        start_time = klines[-1][0] + 1

        if len(klines) < LIMIT:
            break

        time.sleep(0.05)

    if all_klines:
        save_to_sqlite(symbol, all_klines)

    return symbol, len(all_klines)


# ================= 8. 主任务 =================
def main(universe):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_symbol, s) for s in universe]

        for future in as_completed(futures):
            symbol, count = future.result()
            if count == 0:
                print(f"{symbol}: {count}")


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

    print(f"🚀 Alpha ∩ Futures Kline System started ({INTERVAL}, SQLite)")
    print(f"🗄️ DB Path: {DB_PATH}")

    scheduler.start()
