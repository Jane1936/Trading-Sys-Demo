import app


class RecordingScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append((func, trigger, kwargs))

    def start(self):
        self.started = True


def test_alpha_observer_runs_at_thirty_seconds_after_every_utc_hour(monkeypatch):
    scheduler = RecordingScheduler()
    collection_calls = []

    monkeypatch.setattr(app.alpha_observer, "init_db", lambda: None)
    monkeypatch.setattr(app.alpha_observer, "backfill_recent_hours", lambda: 0)
    monkeypatch.setattr(
        app.alpha_observer,
        "collect_snapshot",
        lambda: collection_calls.append("snapshot") or 1,
    )
    monkeypatch.setattr(app.alpha_observer, "collect_hourly_market_data", lambda: 1)
    monkeypatch.setattr(app.alpha_observer, "backfill_recent_daily_klines", lambda: 0)
    monkeypatch.setattr(app.collector, "BlockingScheduler", lambda: scheduler)

    app.start_alpha_observer_task()

    assert collection_calls == ["snapshot"]
    assert scheduler.started is True
    assert len(scheduler.jobs) == 1
    _, trigger, options = scheduler.jobs[0]
    assert trigger == "cron"
    assert options == {
        "minute": 0,
        "second": 30,
        "timezone": "UTC",
        "max_instances": 1,
        "coalesce": True,
    }
