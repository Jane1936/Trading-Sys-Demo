import sqlite3
import json
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collector
import db_config
import app as worker_app
import web_app
from holding_position_scoring import HoldingPositionScoringSystem
from scoring_system import ScoringSystem
from trading_experiment import TradingExperiment
from sqlite_recovery import (
    ensure_sqlite_database_usable,
    is_malformed_database_error,
    is_sqlite_integrity_failure,
    quick_check_sqlite_database,
)


def test_connect_sqlite_initializes_wal_once_per_database_file(tmp_path, monkeypatch):
    db_path = str(tmp_path / "wal-once.db")
    calls = []
    original = db_config.configure_sqlite_connection

    def record_configuration(conn, *, wal=True, initialize_wal=True):
        calls.append((wal, initialize_wal))
        return original(conn, wal=wal, initialize_wal=initialize_wal)

    monkeypatch.setattr(db_config, "configure_sqlite_connection", record_configuration)
    db_config._wal_initialized_files.pop(str(Path(db_path).resolve()), None)

    with db_config.connect_sqlite(db_path):
        pass
    with db_config.connect_sqlite(db_path):
        pass

    assert calls == [(True, True), (True, False)]


def test_ensure_sqlite_database_usable_quarantines_malformed_database(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    wal_path = tmp_path / "klines.db-wal"
    wal_path.write_text("wal")

    quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True, once_per_process=False)

    assert not db_path.exists()
    assert not wal_path.exists()
    assert len(quarantined) >= 2
    assert any(path.endswith("klines.db" + path[path.index(".corrupt-"):]) for path in quarantined)
    assert all(".corrupt-" in path for path in quarantined)


def test_ensure_sqlite_database_usable_skips_quick_check_by_default(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")

    quarantined = ensure_sqlite_database_usable(str(db_path))

    assert quarantined == []
    assert db_path.read_bytes() == b"not a sqlite database"


def test_ensure_sqlite_database_usable_checks_path_once_per_process(tmp_path):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")

    first_quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True)
    db_path.write_bytes(b"not a sqlite database")
    second_quarantined = ensure_sqlite_database_usable(str(db_path), quick_check=True)

    assert first_quarantined
    assert second_quarantined == []
    assert db_path.read_bytes() == b"not a sqlite database"


def test_collector_init_db_recreates_after_quarantining_malformed_database(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))

    collector.init_db()

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (collector.BTC_5M_TABLE,)).fetchone()


def test_web_before_request_reports_without_replacing_live_database(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "_db_recovery_checked_path", None)
    monkeypatch.setattr(web_app, "WEB_SQLITE_QUICK_CHECK_ON_REQUEST", True)

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    assert db_path.read_bytes() == b"not a sqlite database"


def test_web_before_request_skips_quick_check_by_default(tmp_path, monkeypatch):
    db_path = tmp_path / "klines.db"
    db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(web_app, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DB_PATH", str(db_path))
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "_db_recovery_checked_path", None)
    monkeypatch.setattr(web_app, "WEB_SQLITE_QUICK_CHECK_ON_REQUEST", False)

    response = web_app.app.test_client().get("/")

    assert response.status_code == 200
    assert db_path.read_bytes() == b"not a sqlite database"


def test_is_malformed_database_error_matches_sqlite_message():
    assert is_malformed_database_error(sqlite3.DatabaseError("database disk image is malformed"))
    assert not is_malformed_database_error(sqlite3.DatabaseError("database is locked"))


@pytest.mark.parametrize("detail", ["disk I/O error", "database or disk is full"])
def test_worker_health_check_does_not_quarantine_storage_failure(
    tmp_path, monkeypatch, detail
):
    db_path = tmp_path / "scoring.db"
    original = b"database evidence must remain in place"
    db_path.write_bytes(original)
    initialized = []
    monkeypatch.setattr(
        worker_app,
        "_database_initializers",
        lambda: {str(db_path): lambda: initialized.append(True)},
    )
    monkeypatch.setattr(
        worker_app, "quick_check_sqlite_database", lambda _path: (False, detail)
    )

    assert worker_app.check_worker_databases() == {}
    assert db_path.read_bytes() == original
    assert initialized == []
    assert not list(tmp_path.glob("scoring.db.corrupt-*"))
    assert not Path(db_config.database_recovery_marker(str(db_path))).exists()


def test_integrity_failure_classifier_rejects_io_errors():
    assert is_sqlite_integrity_failure("database disk image is malformed")
    assert is_sqlite_integrity_failure("*** in database main ***\nPage 3 is never used")
    assert is_sqlite_integrity_failure(
        "row 42 missing from index sqlite_autoindex_symbol_scores_close_gt_ma20_1"
    )
    assert is_sqlite_integrity_failure("wrong # of entries in index scores_by_round")
    assert not is_sqlite_integrity_failure("disk I/O error")


def test_quick_check_returns_every_integrity_error(tmp_path, monkeypatch):
    class Result:
        def fetchall(self):
            return [
                ("row 1 missing from index first",),
                ("wrong # of entries in index second",),
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, statement):
            assert statement == "PRAGMA quick_check"
            return Result()

    db_path = tmp_path / "scoring.db"
    db_path.write_bytes(b"placeholder")
    monkeypatch.setattr(sqlite3, "connect", lambda *_args, **_kwargs: Connection())

    ok, detail = quick_check_sqlite_database(str(db_path))

    assert not ok
    assert detail == "row 1 missing from index first\nwrong # of entries in index second"


def test_worker_reindexes_before_destructive_recovery(tmp_path, monkeypatch):
    db_path = tmp_path / "scoring.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE scores (symbol TEXT PRIMARY KEY, score INTEGER)")
        conn.execute("INSERT INTO scores VALUES ('BTCUSDT', 10)")
    initialized = []
    monkeypatch.setattr(
        worker_app,
        "_database_initializers",
        lambda: {str(db_path): lambda: initialized.append(True)},
    )
    monkeypatch.setattr(
        worker_app,
        "quick_check_sqlite_database",
        lambda _path: (False, "row 1 missing from index sqlite_autoindex_scores_1"),
    )
    monkeypatch.setattr(
        worker_app, "reindex_sqlite_database", lambda _path: (True, "ok")
    )
    monkeypatch.setenv("SQLITE_RECOVERY_LOG_DIR", str(tmp_path / "incidents"))

    recovered = worker_app.check_worker_databases()

    assert recovered == {str(db_path): []}
    assert initialized == []
    assert not list(tmp_path.glob("scoring.db.corrupt-*"))
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT * FROM scores").fetchall() == [("BTCUSDT", 10)]
    incident_path = next((tmp_path / "incidents").glob("*.json"))
    incident = json.loads(incident_path.read_text(encoding="utf-8"))
    assert incident["status"] == "reindexed"


def test_safe_page_module_does_not_quarantine_live_database(tmp_path, monkeypatch):
    base_db_path = tmp_path / "base.db"
    scoring_db_path = tmp_path / "scoring.db"
    scoring_db_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(db_config, "BASE_DB_PATH", str(base_db_path))
    monkeypatch.setattr(
        db_config,
        "DB_LABELS",
        {"base": str(base_db_path), "scoring": str(scoring_db_path)},
    )
    scoring = ScoringSystem(db_path=str(scoring_db_path))

    result, error = web_app._safe_page_module("评分表初始化", scoring.init_table, None)

    assert result is None
    assert error is not None
    assert "已禁止新业务访问" in error["error"]
    assert scoring_db_path.read_bytes() == b"not a sqlite database"
    assert not list(tmp_path.glob("scoring.db.corrupt-*"))
    assert Path(db_config.database_recovery_marker(str(scoring_db_path))).exists()


def test_worker_health_check_fences_and_replaces_malformed_database(tmp_path, monkeypatch):
    db_paths = [tmp_path / f"db-{index}.sqlite" for index in range(4)]
    for db_path in db_paths:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE healthy (value INTEGER)")
    damaged_path = db_paths[2]
    damaged_path.write_bytes(b"not a sqlite database")
    incident_dir = tmp_path / "incidents"
    monkeypatch.setenv("SQLITE_RECOVERY_LOG_DIR", str(incident_dir))

    def initialize():
        with db_config.connect_sqlite(str(damaged_path)) as conn:
            conn.execute("CREATE TABLE recovered (value INTEGER)")

    monkeypatch.setattr(worker_app, "_database_initializers", lambda: {
        str(path): (initialize if path == damaged_path else lambda: None) for path in db_paths
    })

    recovered = worker_app.check_worker_databases()

    assert list(recovered) == [str(damaged_path)]
    with sqlite3.connect(damaged_path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT 1 FROM recovered").fetchall() == []
    assert list(tmp_path.glob(f"{damaged_path.name}.corrupt-*"))
    assert not Path(db_config.database_recovery_marker(str(damaged_path))).exists()
    records = list(incident_dir.glob("*.json"))
    assert len(records) == 1
    incident = json.loads(records[0].read_text(encoding="utf-8"))
    assert incident["status"] == "recovered"
    assert incident["source"] == "periodic_health_check"
    assert incident["database"]["path"] == str(damaged_path.resolve())
    assert incident["database"]["health"]["status"] == "error"
    assert incident["quarantined_files"]


@pytest.mark.parametrize("detail", ["database is locked", "database is busy"])
def test_worker_health_check_does_not_replace_contended_database(
    tmp_path, monkeypatch, detail
):
    db_path = tmp_path / "trading.db"
    db_path.write_bytes(b"healthy database placeholder")
    initialized = []
    monkeypatch.setattr(
        worker_app, "_database_initializers", lambda: {str(db_path): lambda: initialized.append(True)}
    )
    monkeypatch.setattr(
        worker_app, "quick_check_sqlite_database", lambda _path: (False, detail)
    )

    recovered = worker_app.check_worker_databases()

    assert recovered == {}
    assert initialized == []
    assert db_path.read_bytes() == b"healthy database placeholder"
    assert not Path(db_config.database_recovery_marker(str(db_path))).exists()


def test_worker_malformed_error_triggers_immediate_health_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_app,
        "check_worker_databases",
        lambda **kwargs: calls.append(kwargs),
    )

    assert worker_app.recover_after_worker_error(
        sqlite3.DatabaseError("database disk image is malformed")
    )
    assert calls[0]["source"] == "runtime_exception"
    assert isinstance(calls[0]["trigger_exception"], sqlite3.DatabaseError)
    assert not worker_app.recover_after_worker_error(sqlite3.DatabaseError("database is locked"))
    assert len(calls) == 1


def test_worker_database_health_check_interval_is_five_minutes():
    assert worker_app.DATABASE_HEALTH_CHECK_INTERVAL_SEC == 5 * 60


def test_base_database_recovery_recreates_indicator_tables(tmp_path, monkeypatch):
    base_db = str(tmp_path / "base_data.db")
    monkeypatch.setattr(db_config, "BASE_DB_PATH", base_db)
    monkeypatch.setattr(collector, "DB_PATH", base_db)
    monkeypatch.setattr(collector, "DATA_DIR", str(tmp_path))

    worker_app._database_initializers()[base_db]()

    with sqlite3.connect(base_db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert {"ma20_indicators", "ema_indicators", "macd_indicators"} <= tables


def test_trading_core_recovery_recreates_holding_risk_tables(tmp_path, monkeypatch):
    trading_db = str(tmp_path / "trading.db")
    trading_core_db = str(tmp_path / "trading_core.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", trading_core_db)

    worker_app._database_initializers()[trading_core_db]()

    scoring = HoldingPositionScoringSystem(db_path=trading_db)
    expected_tables = {
        scoring.CHECKS_TABLE,
        scoring.RECORDS_TABLE,
        scoring.PORTFOLIO_RISK_TABLE,
        scoring.PORTFOLIO_RISK_SUMMARY_TABLE,
        scoring.REDUCTION_CHECKS_TABLE,
        scoring.REDUCTION_RECORDS_TABLE,
    }
    with sqlite3.connect(trading_core_db) as conn:
        actual_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert expected_tables <= actual_tables


def test_startup_initializes_and_verifies_every_database(tmp_path, monkeypatch):
    first = str(tmp_path / "first.db")
    second = str(tmp_path / "second.db")
    calls = []

    def initializer(path, table):
        def initialize():
            calls.append(path)
            with db_config.connect_sqlite(path) as conn:
                conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY)")
        return initialize

    monkeypatch.setattr(worker_app, "check_worker_databases", lambda **kwargs: calls.append(kwargs["source"]))
    monkeypatch.setattr(worker_app, "_database_initializers", lambda: {
        first: initializer(first, "first_table"),
        second: initializer(second, "second_table"),
    })
    monkeypatch.setattr(worker_app, "_database_schema_requirements", lambda: {
        first: {"first_table": {"id"}},
        second: {"second_table": {"id"}},
    })

    worker_app.initialize_worker_databases()

    assert calls == ["startup_health_check", first, second]


def test_schema_verification_rejects_missing_required_column(tmp_path, monkeypatch):
    db_path = str(tmp_path / "incomplete.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE lifecycle (id INTEGER PRIMARY KEY)")
    monkeypatch.setattr(worker_app, "_database_schema_requirements", lambda: {
        db_path: {"lifecycle": {"id", "status"}}
    })

    with pytest.raises(RuntimeError, match="lifecycle missing columns status"):
        worker_app.verify_database_schema(db_path)


def test_schema_contract_routes_trailing_reduction_tables_to_owner_databases(
    tmp_path, monkeypatch
):
    trading_db = str(tmp_path / "trading.db")
    info_db = str(tmp_path / "trading_info.db")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", info_db)

    requirements = worker_app._database_schema_requirements()

    assert worker_app.TrailingReductionTracker.CHECKS_TABLE in requirements[trading_db]
    assert worker_app.TrailingReductionTracker.CHECKS_TABLE not in requirements[info_db]
    assert worker_app.TrailingReductionTracker.RECORDS_TABLE in requirements[info_db]


def test_malformed_trading_core_recovery_rebuilds_and_verifies_schema(tmp_path, monkeypatch):
    trading_db = str(tmp_path / "trading.db")
    trading_core_db = str(tmp_path / "trading_core.db")
    trading_core_path = Path(trading_core_db)
    trading_core_path.write_bytes(b"not a sqlite database")
    monkeypatch.setattr(db_config, "TRADING_DB_PATH", trading_db)
    monkeypatch.setattr(db_config, "TRADING_CORE_DB_PATH", trading_core_db)
    monkeypatch.setattr(db_config, "TRADING_INFO_DB_PATH", str(tmp_path / "trading_info.db"))
    monkeypatch.setattr(worker_app, "_database_initializers", lambda: {
        trading_core_db: lambda: (
            TradingExperiment(db_path=trading_db).init_core_tables(),
            HoldingPositionScoringSystem(db_path=trading_db).init_tables(),
            worker_app.ZombieForceLiquidationModule(db_path=trading_db).init_tables(),
        )
    })

    recovered = worker_app.check_worker_databases(source="integration_test")

    assert list(recovered) == [trading_core_db]
    worker_app.verify_database_schema(trading_core_db)
    with sqlite3.connect(trading_core_db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert list(tmp_path.glob("trading_core.db.corrupt-*"))
    assert not Path(db_config.database_recovery_marker(trading_core_db)).exists()


def test_recovery_marker_blocks_business_connections(tmp_path):
    db_path = str(tmp_path / "scoring.db")
    Path(db_config.database_recovery_marker(db_path)).write_text("recovering")

    with pytest.raises(db_config.DatabaseRecoveringError):
        db_config.connect_sqlite(db_path)

    with db_config.sqlite_recovery_bypass(db_path):
        with db_config.connect_sqlite(db_path) as conn:
            conn.execute("CREATE TABLE recovered (value INTEGER)")


def test_exclusive_recovery_waits_for_existing_connection_to_close(tmp_path):
    db_path = str(tmp_path / "scoring.db")
    conn = db_config.connect_sqlite(db_path)
    acquired = threading.Event()

    def acquire_exclusive():
        with db_config.sqlite_access_lock(db_path, exclusive=True):
            acquired.set()

    thread = threading.Thread(target=acquire_exclusive)
    thread.start()
    time.sleep(0.05)
    assert not acquired.is_set()

    conn.close()
    thread.join(timeout=1)
    assert acquired.is_set()
