import collector


def test_kline_job_runs_market_filter_sources_in_main_flow(monkeypatch):
    calls = []

    monkeypatch.setattr(collector, "UNIVERSE", ["ETH"])
    monkeypatch.setattr(collector, "kline_job_running", False)
    monkeypatch.setattr(collector, "run_kline_main", lambda universe: calls.append(("universe_klines", tuple(universe))))
    monkeypatch.setattr(collector, "run_btc_15m_main", lambda: calls.append(("btc_15m",)))
    monkeypatch.setattr(
        collector.allusdt_15m_ma20,
        "run",
        lambda db_path, db_write_lock=None: calls.append(("allusdt_15m_ma20", db_path, db_write_lock)),
    )

    collector.kline_job()

    assert calls == [
        ("universe_klines", ("ETH",)),
        ("btc_15m",),
        ("allusdt_15m_ma20", collector.DB_PATH, collector.db_write_lock),
    ]
    assert collector.kline_job_running is False
