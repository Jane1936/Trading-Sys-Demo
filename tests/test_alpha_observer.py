import sqlite3

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
