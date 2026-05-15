"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
- pre-safety task: detect abnormal wick events every 15m decision round
"""

from __future__ import annotations

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

_universe_lock = threading.Lock()


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

    last_round_ts = None
    round_ms = 15 * 60_000

    print(f"🛡️ Pre-safety task started, symbols={len(symbols)}")
    while True:
        now_ms = int(time.time() * 1000)
        round_ts = (now_ms // round_ms) * round_ms

        if round_ts != last_round_ts:
            for symbol in symbols:
                try:
                    event = module.detect_for_symbol(symbol, now_ms=now_ms)
                    if event is not None:
                        print(
                            f"🚨 abnormal wick {event.symbol} "
                            f"round={event.decision_round_ts} "
                            f"first_open={event.first_candle_open_time} "
                            f"cond1={event.cond1_ratio:.6f} cond2={event.cond2_ratio:.6f}"
                        )
                except Exception as exc:  # keep this side-task isolated
                    print(f"⚠️ pre-safety detect failed symbol={symbol}: {exc}")

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
    # 预先构建一次 universe，避免多线程重复构建造成耦合
    symbols = ensure_universe()

    # 三个独立 task：collector / pre_safety / data_processor
    collector_thread = threading.Thread(target=start_collector_task, args=(symbols,), daemon=True)
    collector_thread.start()

    pre_safety_thread = threading.Thread(target=start_pre_safety_task, args=(symbols,), daemon=True)
    pre_safety_thread.start()

    # 主线程跑 processor task
    start_processor_task(symbols)
