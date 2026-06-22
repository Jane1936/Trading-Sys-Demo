"""Binance USDⓈ-M Futures account balance manager.

This module keeps the account-facing REST logic isolated from the Flask
routes. Configure the placeholder constants below before querying a real
account, or override them with environment variables in Docker/local shells.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import requests


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else value


# ===== Binance account configuration placeholders =====
# Fill these placeholders locally. Do not commit real API keys/secrets.
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "true").strip().lower() in {"1", "true", "yes", "on"}

TESTNET_API_KEY = _env_or_default("BINANCE_TESTNET_API_KEY", "YOUR_TESTNET_API_KEY")
TESTNET_SECRET_KEY = _env_or_default("BINANCE_TESTNET_SECRET_KEY", "YOUR_TESTNET_SECRET_KEY")

REAL_API_KEY = _env_or_default("BINANCE_REAL_API_KEY", "YOUR_REAL_API_KEY")
REAL_API_SECRET = _env_or_default("BINANCE_REAL_API_SECRET", "YOUR_REAL_API_SECRET")

if BINANCE_TESTNET:
    BASE_URL = _env_or_default("BINANCE_BASE_URL", "https://demo-fapi.binance.com")
    API_KEY = TESTNET_API_KEY
    SECRET_KEY = TESTNET_SECRET_KEY
else:
    BASE_URL = _env_or_default("BINANCE_BASE_URL", "https://fapi.binance.com")
    API_KEY = REAL_API_KEY
    SECRET_KEY = REAL_API_SECRET

BALANCE_ENDPOINT = "/fapi/v3/balance"
DEFAULT_RECV_WINDOW = 5000
PLACEHOLDER_VALUES = {
    "",
    "YOUR_TESTNET_API_KEY",
    "YOUR_TESTNET_SECRET_KEY",
    "YOUR_REAL_API_KEY",
    "YOUR_REAL_API_SECRET",
}


class BinanceAccountConfigError(RuntimeError):
    """Raised when account API credentials are not configured."""


@dataclass(frozen=True)
class BinanceFilledSellOrderRow:
    """Normalized filled SELL trade row returned to the web layer."""

    symbol: str
    order_id: str
    trade_id: str
    time: int
    side: str
    price: str
    quantity: str
    quote_quantity: str
    realized_pnl: str
    commission: str
    commission_asset: str
    maker: bool

@dataclass(frozen=True)
class BinanceBalanceRow:
    """Normalized balance row returned to the web layer."""

    asset: str
    balance: str
    available_balance: str
    cross_wallet_balance: str
    cross_un_pnl: str
    account_alias: str


class BinanceAccountManager:
    """Small REST client for signed Binance USDⓈ-M Futures account calls."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        api_key: str = API_KEY,
        secret_key: str = SECRET_KEY,
        recv_window: int = DEFAULT_RECV_WINDOW,
        timeout: int = 10,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.recv_window = recv_window
        self.timeout = timeout
        self.session = requests.Session()

    def validate_config(self) -> None:
        """Validate that API credentials are no longer placeholder values."""
        if self.api_key in PLACEHOLDER_VALUES or self.secret_key in PLACEHOLDER_VALUES:
            raise BinanceAccountConfigError(
                "Binance API credentials are not configured. Fill the placeholders in "
                "binance_account_manager.py or set BINANCE_TESTNET_API_KEY/"
                "BINANCE_TESTNET_SECRET_KEY environment variables."
            )

    def futures_balance(self) -> dict[str, Any]:
        """Query and normalize USDⓈ-M Futures account balances."""
        self.validate_config()
        raw_rows = self._signed_get(BALANCE_ENDPOINT)
        if not isinstance(raw_rows, list):
            raise RuntimeError("Unexpected Binance balance response format")

        balances = [self._normalize_balance_row(row) for row in raw_rows if isinstance(row, dict)]
        return {
            "testnet": BINANCE_TESTNET,
            "base_url": self.base_url,
            "queried_at": int(time.time() * 1000),
            "balances": [row.__dict__ for row in balances],
        }

    def futures_recent_filled_sell_orders(self, days: int = 7, limit: int = 1000) -> dict[str, Any]:
        """Query recent filled SELL trades from USDⓈ-M Futures account trade history."""
        self.validate_config()
        now_ms = int(time.time() * 1000)
        days = max(1, min(int(days), 30))
        limit = max(1, min(int(limit), 1000))
        start_time = now_ms - days * 24 * 60 * 60 * 1000
        raw_rows = self._signed_get(
            "/fapi/v1/userTrades",
            {"startTime": start_time, "endTime": now_ms, "limit": limit},
        )
        if not isinstance(raw_rows, list):
            raise RuntimeError("Unexpected Binance user trades response format")

        orders = [
            self._normalize_filled_sell_order_row(row)
            for row in raw_rows
            if isinstance(row, dict) and not bool(row.get("buyer", False))
        ]
        orders.sort(key=lambda row: row.time, reverse=True)
        return {
            "testnet": BINANCE_TESTNET,
            "base_url": self.base_url,
            "queried_at": now_ms,
            "days": days,
            "start_time": start_time,
            "end_time": now_ms,
            "orders": [row.__dict__ for row in orders],
        }

    def _signed_get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{self.base_url}{endpoint}",
            params=self._signed_params(params),
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=self.timeout,
        )
        self._raise_for_status(response)
        return response.json()

    def _signed_post(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.post(
            f"{self.base_url}{endpoint}",
            params=self._signed_params(params),
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=self.timeout,
        )
        self._raise_for_status(response)
        return response.json()

    def _signed_delete(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.delete(
            f"{self.base_url}{endpoint}",
            params=self._signed_params(params),
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=self.timeout,
        )
        self._raise_for_status(response)
        return response.json()

    def _public_get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{self.base_url}{endpoint}",
            params=dict(params or {}),
            timeout=self.timeout,
        )
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            body = response.text.strip()
            if body:
                exc.args = (*exc.args, f"response_body={body}")
            raise

    def _signed_params(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_params: dict[str, Any] = dict(params or {})
        request_params["timestamp"] = int(time.time() * 1000)
        request_params["recvWindow"] = self.recv_window
        query_string = urlencode(request_params)
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        request_params["signature"] = signature
        return request_params

    @staticmethod
    def _normalize_filled_sell_order_row(row: dict[str, Any]) -> BinanceFilledSellOrderRow:
        price = str(row.get("price", "0"))
        quantity = str(row.get("qty", row.get("quantity", "0")))
        quote_quantity = str(row.get("quoteQty", ""))
        if quote_quantity == "":
            try:
                quote_quantity = str(Decimal(price) * Decimal(quantity))
            except Exception:
                quote_quantity = "0"
        return BinanceFilledSellOrderRow(
            symbol=str(row.get("symbol", "")),
            order_id=str(row.get("orderId", "")),
            trade_id=str(row.get("id", "")),
            time=int(row.get("time", 0) or 0),
            side="SELL",
            price=price,
            quantity=quantity,
            quote_quantity=quote_quantity,
            realized_pnl=str(row.get("realizedPnl", "0")),
            commission=str(row.get("commission", "0")),
            commission_asset=str(row.get("commissionAsset", "")),
            maker=bool(row.get("maker", False)),
        )

    @staticmethod
    def _normalize_balance_row(row: dict[str, Any]) -> BinanceBalanceRow:
        return BinanceBalanceRow(
            asset=str(row.get("asset", "")),
            balance=str(row.get("balance", "0")),
            available_balance=str(row.get("availableBalance", "0")),
            cross_wallet_balance=str(row.get("crossWalletBalance", "0")),
            cross_un_pnl=str(row.get("crossUnPnl", "0")),
            account_alias=str(row.get("accountAlias", "")),
        )
