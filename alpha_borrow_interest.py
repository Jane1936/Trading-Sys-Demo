"""Hourly VIP 0 margin borrowing-rate observations (decimal daily rates)."""
from __future__ import annotations

import json
import os
import time
from decimal import Decimal, InvalidOperation

import requests
import db_config

SOURCE_URL = 'https://www.binance.com/zh-CN/margin-fee#borrowinterest'
API_URL = os.getenv('ALPHA_BORROW_INTEREST_URL', 'https://www.binance.com/bapi/margin/v1/friendly/margin/vip/spec/list-all')
TABLE = 'alpha_borrow_interest_rates'


def init_db(db_path=None):
    with db_config.connect_sqlite(db_path or db_config.ALPHA_DB_PATH) as conn:
        conn.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (
            symbol TEXT NOT NULL, vip_level INTEGER NOT NULL,
            observed_at INTEGER NOT NULL, fetched_at INTEGER NOT NULL,
            daily_interest_rate TEXT NOT NULL, source_url TEXT NOT NULL,
            annual_interest_rate TEXT NOT NULL DEFAULT '', payload_json TEXT NOT NULL,
            PRIMARY KEY(symbol, vip_level, observed_at))''')
        columns = {row[1] for row in conn.execute(f'PRAGMA table_info({TABLE})')}
        if 'annual_interest_rate' not in columns:
            conn.execute(f"ALTER TABLE {TABLE} ADD COLUMN annual_interest_rate TEXT NOT NULL DEFAULT ''")
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{TABLE}_time ON {TABLE}(observed_at)')


def parse_rates(payload):
    if not isinstance(payload, dict) or str(payload.get('code')) != '000000':
        raise ValueError('Binance borrowing-rate API returned an unsuccessful response')
    data = payload.get('data')
    if not isinstance(data, list) or not data:
        raise ValueError('Binance borrowing-rate API returned no rate list')
    result = {}
    for group in data:
        if not isinstance(group, dict):
            raise ValueError('Invalid borrowing-rate record')
        asset_name = str(group.get('assetName') or group.get('asset') or group.get('coin') or '').strip().upper()
        records = group.get('specs')
        if records is None:
            records = [group]
        if not isinstance(records, list):
            raise ValueError('Invalid borrowing-rate specs')
        for row in records:
            if not isinstance(row, dict) or str(row.get('vipLevel')) != '0':
                continue
            symbol = str(row.get('assetName') or row.get('asset') or row.get('coin') or asset_name).strip().upper()
            value = row.get('dailyInterestRate')
            if not symbol or value is None:
                raise ValueError('Missing asset or dailyInterestRate in VIP 0 response')
            try:
                rate = Decimal(str(value))
            except InvalidOperation as exc:
                raise ValueError('Invalid daily interest rate') from exc
            if not rate.is_finite() or rate < 0:
                raise ValueError('Invalid daily interest rate')
            result[symbol] = (str(rate), row)
    if not result:
        raise ValueError('No VIP 0 borrowing rates found')
    return result


def collect_once(db_path=None, *, session=requests, observed_at=None):
    # Do not hold any database connection/transaction across the HTTP request.
    response = session.get(API_URL, timeout=30)
    response.raise_for_status()
    rates = parse_rates(response.json())
    fetched_at = int(time.time() * 1000)
    bucket = int(observed_at if observed_at is not None else fetched_at) // 3_600_000 * 3_600_000
    path = db_path or db_config.ALPHA_DB_PATH
    init_db(path)
    with db_config.connect_sqlite(path) as conn:
        conn.executemany(f'''INSERT INTO {TABLE}
            (symbol, vip_level, observed_at, fetched_at, daily_interest_rate,
             source_url, annual_interest_rate, payload_json)
            VALUES (?, 0, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, vip_level, observed_at) DO UPDATE SET
            fetched_at=excluded.fetched_at, daily_interest_rate=excluded.daily_interest_rate,
            annual_interest_rate=excluded.annual_interest_rate,
            payload_json=excluded.payload_json''',
            [(symbol, bucket, fetched_at, rate, SOURCE_URL,
              str(Decimal(rate) * Decimal('365')), json.dumps(raw, ensure_ascii=False))
             for symbol, (rate, raw) in rates.items()])
    return len(rates)
