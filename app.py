"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
- pre-safety task: detect abnormal wick events every 15m decision round
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import List

import collector
from data_processor import (
    MA20Processor,
    MA20Scheduler,
    MACalcResult,
    init_ma20_table,
    run_loop,
    save_ma20_result,
)
from pre_safety_module import PreSafetyModule
from scoring_system import ScoringSystem

_universe_lock = threading.Lock()


def verify_db_writable(db_path: str) -> None:
    """Fail fast with actionable diagnostics when DB path is not writable."""
    data_dir = collector.DATA_DIR

    try:
        collector.init_db()
    except sqlite3.OperationalError as exc:
        if "readonly" not in str(exc).lower():
            raise
        dir_writable = os.access(data_dir, os.W_OK)
        file_exists = os.path.exists(db_path)
        file_writable = os.access(db_path, os.W_OK) if file_exists else False
        raise RuntimeError(
            "Database is readonly. "
            f"db_path={db_path}, data_dir={data_dir}, "
            f"data_dir_writable={dir_writable}, "
            f"db_exists={file_exists}, db_writable={file_writable}. "
            "Check host volume permissions (chown/chmod) for the mounted ./data directory."
        ) from exc


def ensure_universe() -> List[str]:
    """Build universe once and return a stable symbol list snapshot."""
    with _universe_lock:
        if collector.UNIVERSE is None:
            collector.UNIVERSE = collector.build_universe()
        return list(collector.UNIVERSE)


def start_pre_safety_task(symbols: List[str]) -> None:
    """Run pre-safety abnormal wick detection in an isolated daemon thread.

    This task only reads existing 5m candle data and writes its own event table,
    so it will not interfere with collector/MA20 pipelines.
    """
    module = PreSafetyModule(db_path=collector.DB_PATH)
    module.init_table()
    scoring = ScoringSystem(db_path=collector.DB_PATH)
    scoring.init_table()

    last_round_ts = None
    round_ms = 15 * 60_000

    print(f"🛡️ Pre-safety task started, symbols={len(symbols)}")
    while True:
        now_ms = int(time.time() * 1000)
        round_ts = (now_ms // round_ms) * round_ms

        if round_ts != last_round_ts:
            for symbol in symbols:
                try:
                    events = module.detect_for_symbol(symbol, now_ms=now_ms)
                    for event in events:
                        print(
                            f"🚨 abnormal wick {event.symbol} "
                            f"round={event.decision_round_ts} "
                            f"candle_index={event.candle_index} "
                            f"first_open={event.first_candle_open_time} "
                            f"cond1={event.cond1_ratio:.6f} cond2={event.cond2_ratio:.6f}"
                        )
                except Exception as exc:  # keep this side-task isolated
                    print(f"⚠️ pre-safety detect failed symbol={symbol}: {exc}")

            try:
                _, abnormal_symbols = module.get_latest_round_abnormal_symbols(decision_round_ts=round_ts)
                scored = scoring.score_round(
                    decision_round_ts=round_ts,
                    all_symbols=symbols,
                    abnormal_symbols=abnormal_symbols,
                )
                print(
                    f"🧮 scoring round={round_ts} universe={len(symbols)} "
                    f"abnormal={len(set(abnormal_symbols))} scored={len(scored)}"
                )
            except Exception as exc:
                print(f"⚠️ scoring failed round={round_ts}: {exc}")

            last_round_ts = round_ts

        time.sleep(5)


def on_ma20_result(result: MACalcResult) -> None:
    save_ma20_result(collector.DB_PATH, result)
    print(
        f"📈 MA20 {result.symbol} {result.interval} "
        f"open_time={result.open_time} close={result.close:.6f} ma20={result.ma20:.6f}"
    )


def start_collector_task(symbols: List[str]) -> None:
    collector.init_db()
    collector.UNIVERSE = list(symbols)

    scheduler = collector.BlockingScheduler()
    scheduler.add_job(collector.kline_job, "cron", second=0)
    scheduler.add_job(collector.oi_job, "cron", second=20)
    scheduler.add_job(collector.funding_job, "cron", minute=1, second=40)
    scheduler.add_job(collector.btc_5m_job, "cron", minute="*/5", second=10)

    print("🚀 Collector task started")
    scheduler.start()


def start_processor_task(symbols: List[str]) -> None:
    init_ma20_table(db_path=collector.DB_PATH)
    processor = MA20Processor(db_path=collector.DB_PATH)
    scheduler = MA20Scheduler(grace_seconds=5)

    print(f"🚀 MA20 processor task started, symbols={len(symbols)}")
    run_loop(
        symbols=symbols,
        processor=processor,
        scheduler=scheduler,
        on_result=on_ma20_result,
        poll_seconds=20,
    )


if __name__ == "__main__":
    verify_db_writable(collector.DB_PATH)
    # 预先构建一次 universe，避免多线程重复构建造成耦合
    symbols = ensure_universe()

    # 三个独立 task：collector / pre_safety / data_processor
    collector_thread = threading.Thread(target=start_collector_task, args=(symbols,), daemon=True)
    collector_thread.start()

    pre_safety_thread = threading.Thread(target=start_pre_safety_task, args=(symbols,), daemon=True)
    pre_safety_thread.start()

    # 主线程跑 processor task
    start_processor_task(symbols)
