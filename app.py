"""Project entrypoint: run collector task + MA20 processor task together.

- collector task: fetch 1m data and aggregate to 15m/1h/... by schedule
- data processor task: consume DB aggregated candles and emit MA20 updates
"""

from __future__ import annotations

import threading

import collector
from data_processor import (
    MA20Processor,
    MA20Scheduler,
    MACalcResult,
    init_ma20_table,
    run_loop,
    save_ma20_result,
)


def on_ma20_result(result: MACalcResult) -> None:
    save_ma20_result(collector.DB_PATH, result)
    print(
        f"📈 MA20 {result.symbol} {result.interval} "
        f"open_time={result.open_time} close={result.close:.6f} ma20={result.ma20:.6f}"
    )


def start_collector_task() -> None:
    collector.init_db()
    if collector.UNIVERSE is None:
        collector.UNIVERSE = collector.build_universe()

    scheduler = collector.BlockingScheduler()
    scheduler.add_job(collector.kline_job, "cron", second=0)
    scheduler.add_job(collector.oi_job, "cron", second=20)
    scheduler.add_job(collector.funding_job, "cron", minute=1, second=40)

    print("🚀 Collector task started")
    scheduler.start()


def start_processor_task() -> None:
    # Reuse universe created by collector logic.
    if collector.UNIVERSE is None:
        collector.UNIVERSE = collector.build_universe()

    init_ma20_table(db_path=collector.DB_PATH)
    processor = MA20Processor(db_path=collector.DB_PATH)
    scheduler = MA20Scheduler(grace_seconds=5)

    print(f"🚀 MA20 processor task started, symbols={len(collector.UNIVERSE)}")
    run_loop(
        symbols=collector.UNIVERSE,
        processor=processor,
        scheduler=scheduler,
        on_result=on_ma20_result,
        poll_seconds=20,
    )


if __name__ == "__main__":
    # 两个独立 task：collector / data_processor
    collector_thread = threading.Thread(target=start_collector_task, daemon=True)
    collector_thread.start()

    # 主线程跑 processor task
    start_processor_task()
