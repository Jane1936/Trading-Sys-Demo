import sqlite3

import pytest

import alpha_observer


TOKENS = [
    {
        "symbol": "ALPHA",
        "name": "Alpha",
        "chainId": "56",
        "contractAddress": "0x1",
        "volume24h": "123.4",
        "marketCap": "567.8",
    }
]


class Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {"data": TOKENS}


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response()


def test_fetch_tokens_uses_collector_alpha_source_and_browser_header():
    session = Session()
    assert alpha_observer.fetch_tokens(session) == TOKENS
    url, kwargs = session.calls[0]
    assert "/wallet-direct/buw/wallet/cex/alpha/all/token/list" in url
    assert kwargs["headers"]["User-Agent"] == "Mozilla/5.0"


def test_backfill_recent_hours_fetches_once_and_creates_24_buckets(tmp_path):
    session = Session()
    db_path = str(tmp_path / "alpha.db")
    hour_ms = 60 * 60 * 1000

    inserted = alpha_observer.backfill_recent_hours(
        db_path, session=session, now_ms=100 * hour_ms + 123, hours=24
    )

    assert inserted == 24
    assert len(session.calls) == 1
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT observed_at FROM alpha_market_snapshots ORDER BY observed_at"
        ).fetchall()
    assert rows == [((77 + offset) * hour_ms,) for offset in range(24)]

    assert alpha_observer.backfill_recent_hours(
        db_path, session=session, now_ms=100 * hour_ms + 999, hours=24
    ) == 0

    status = alpha_observer.database_status(db_path)
    assert status.path == str((tmp_path / "alpha.db").resolve())
    assert status.size_bytes > 0
    assert status.total_rows == 24
    assert status.snapshot_count == 24


def test_latest_snapshot_calculates_one_to_five_day_activity(tmp_path):
    db_path = str(tmp_path / "alpha.db")
    alpha_observer.init_db(db_path)
    latest = 10 * alpha_observer.DAY_MS
    with sqlite3.connect(db_path) as conn:
        for day, volume in enumerate((100, 80, 60, 40, 20)):
            conn.execute(
                """INSERT INTO alpha_market_snapshots
                   (symbol, name, chain_id, contract_address, volume_24h,
                    market_cap, observed_at)
                   VALUES ('ALPHA', 'Alpha', '56', '0x1', ?, '200', ?)""",
                (str(volume), latest - day * alpha_observer.DAY_MS),
            )

    observed_at, tokens = alpha_observer.latest_snapshot(db_path)

    assert observed_at == latest
    assert tokens[0]["activity_1d"] == 0.5
    assert tokens[0]["activity_2d"] == 0.9
    assert tokens[0]["activity_3d"] == 1.2
    assert tokens[0]["activity_4d"] == 1.4
    assert tokens[0]["activity_5d"] == 1.5


def test_latest_snapshot_leaves_incomplete_multi_day_activity_empty(tmp_path):
    db_path = str(tmp_path / "alpha.db")
    alpha_observer.collect_snapshot(db_path, session=Session(), observed_at=1000)

    _, tokens = alpha_observer.latest_snapshot(db_path)

    assert tokens[0]["activity_1d"] == 123.4 / 567.8
    assert all(tokens[0][f"activity_{day}d"] is None for day in range(2, 6))


def test_collect_daily_klines_skips_invalid_alpha_symbols(tmp_path):
    class KlineSession:
        def get(self, url, **kwargs):
            if kwargs["params"]["symbol"] == "BADUSDT":
                raise RuntimeError("symbol not found")
            return type("KlineResponse", (), {
                "raise_for_status": lambda self: None,
                "json": lambda self: [[1, "1", "2", "0.5", "1.5", "10", 2]],
            })()

    db_path = str(tmp_path / "alpha.db")
    inserted = alpha_observer.collect_daily_klines(
        db_path, session=KlineSession(), symbols=["BAD", "GOOD"]
    )
    assert inserted == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM alpha_daily_klines").fetchone()[0] == 1


def test_daily_trends_calculates_three_day_return_and_sorts_descending(tmp_path):
    db_path = str(tmp_path / "alpha.db")
    alpha_observer.init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        for symbol in ("GAIN", "LOSS", "SHORT"):
            conn.execute(
                """INSERT INTO alpha_market_snapshots
                   (symbol, observed_at) VALUES (?, 1)""",
                (symbol,),
            )
        candles = {
            "GAIN": [(1, 100, 105), (2, 105, 110), (3, 110, 130)],
            "LOSS": [(1, 100, 95), (2, 95, 90), (3, 90, 80)],
            "SHORT": [(1, 10, 12), (2, 12, 14)],
        }
        for symbol, rows in candles.items():
            for open_time, opening, close in rows:
                conn.execute(
                    """INSERT INTO alpha_daily_klines
                       (symbol, open_time, open, close) VALUES (?, ?, ?, ?)""",
                    (symbol, open_time, opening, close),
                )

    trends = alpha_observer.daily_trends(db_path)

    assert [trend["symbol"] for trend in trends] == ["GAIN", "LOSS", "SHORT"]
    assert trends[0]["three_day_return"] == pytest.approx(0.3)
    assert trends[1]["three_day_return"] == pytest.approx(-0.2)
    assert trends[2]["three_day_return"] is None
