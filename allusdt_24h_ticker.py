"""Retrieve the rolling 24-hour ALLUSDT change used by market views and filters."""

from __future__ import annotations

import requests


BINANCE_FUTURES_24H_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"


def fetch_change_percent() -> float:
    """Return Binance's rolling 24-hour price change as percentage points."""
    response = requests.get(
        BINANCE_FUTURES_24H_TICKER_URL,
        params={"symbol": "ALLUSDT"},
        timeout=(3, 10),
    )
    response.raise_for_status()
    return float(response.json()["priceChangePercent"])
