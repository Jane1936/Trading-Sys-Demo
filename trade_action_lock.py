"""SQLite-backed symbol-level lock for order/cancel actions."""

from __future__ import annotations

import os
import sqlite3
import db_config
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class TradeActionLockHandle:
    symbol: str
    owner: str
    action_type: str
    acquired_at: int
    expires_at: int


class TradeActionLockManager:
    """Coordinate trading actions so one symbol has only one active actor."""

    TABLE = "trade_action_locks"

    def __init__(self, db_path: str = "data/klines.db", ttl_ms: int = 120_000) -> None:
        self.db_path = db_path
        self.ttl_ms = ttl_ms

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        db_config.attach_databases(conn, [("base", db_config.BASE_DB_PATH), ("scoring", db_config.SCORING_DB_PATH), ("market", db_config.MARKET_DB_PATH)])
        return conn

    def init_table(self) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                    symbol TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    acquired_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE}_expires ON {self.TABLE}(expires_at ASC)")

    def acquire(self, symbol: str, owner: str, action_type: str, now_ms: int | None = None) -> TradeActionLockHandle | None:
        self.init_table()
        normalized_symbol = str(symbol).upper().strip()
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        expires_at = now + int(self.ttl_ms)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"DELETE FROM {self.TABLE} WHERE expires_at <= ?", (now,))
            try:
                conn.execute(
                    f"INSERT INTO {self.TABLE} (symbol, owner, action_type, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?)",
                    (normalized_symbol, owner, action_type, now, expires_at),
                )
            except sqlite3.IntegrityError:
                conn.rollback()
                return None
            conn.commit()
        return TradeActionLockHandle(normalized_symbol, owner, action_type, now, expires_at)

    def release(self, handle: TradeActionLockHandle | None) -> None:
        if handle is None:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    f"DELETE FROM {self.TABLE} WHERE symbol = ? AND owner = ? AND action_type = ? AND acquired_at = ?",
                    (handle.symbol, handle.owner, handle.action_type, handle.acquired_at),
                )
        except sqlite3.Error:
            return

    def busy_reason(self, symbol: str, owner: str, action_type: str) -> str:
        normalized_symbol = str(symbol).upper().strip()
        self.init_table()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT owner, action_type, acquired_at, expires_at FROM {self.TABLE} WHERE symbol = ?",
                (normalized_symbol,),
            ).fetchone()
        if not row:
            return f"trade_action_lock_busy: symbol={normalized_symbol}; owner={owner}; action_type={action_type}"
        return (
            "trade_action_lock_busy: "
            f"symbol={normalized_symbol}; requested_owner={owner}; requested_action={action_type}; "
            f"locked_by={row['owner']}; locked_action={row['action_type']}; "
            f"acquired_at={row['acquired_at']}; expires_at={row['expires_at']}"
        )


def acquire_trade_action_lock(db_path: str, symbol: str, owner: str, action_type: str, now_ms: int | None = None) -> tuple[TradeActionLockManager, TradeActionLockHandle | None, str]:
    manager = TradeActionLockManager(db_path=db_path)
    handle = manager.acquire(symbol=symbol, owner=owner, action_type=action_type, now_ms=now_ms)
    if handle is None:
        return manager, None, manager.busy_reason(symbol=symbol, owner=owner, action_type=action_type)
    return manager, handle, ""
