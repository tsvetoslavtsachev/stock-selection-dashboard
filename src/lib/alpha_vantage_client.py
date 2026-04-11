"""
Alpha Vantage client — fetches TIME_SERIES_WEEKLY_ADJUSTED for a symbol.

API key is read from the environment variable ALPHA_VANTAGE_API_KEY.
Set it in GitHub Actions secrets and locally in a .env file (not committed).

Alpha Vantage free tier: 25 requests / day, 5 requests / minute.
The client enforces a configurable per-request delay to stay within limits.
"""

from __future__ import annotations

import os
import time
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.alphavantage.co/query"

# Default delay between requests (seconds).
# Free tier: 5 req/min → 12 s is safe; override for premium keys.
_DEFAULT_DELAY = 12.0


class AlphaVantageClient:
    """
    Thin wrapper around the Alpha Vantage REST API.

    Parameters
    ----------
    api_key : str | None
        Alpha Vantage API key.  If None, reads from the environment variable
        ``ALPHA_VANTAGE_API_KEY``.  Raises ``ValueError`` if neither is set.
    rate_limit_delay : float
        Seconds to sleep after each request.  Default 12 s (free-tier safe).
    """

    def __init__(
        self,
        api_key: str | None = None,
        rate_limit_delay: float = _DEFAULT_DELAY,
    ) -> None:
        resolved_key = api_key or os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Alpha Vantage API key not found. "
                "Set the environment variable ALPHA_VANTAGE_API_KEY or pass api_key= explicitly."
            )
        self._api_key = resolved_key
        self.delay = rate_limit_delay
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, params: dict[str, str], retries: int = 3) -> Any:
        """GET with retry and back-off on transient failures."""
        params["apikey"] = self._api_key
        last_exc: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(_BASE_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                # Alpha Vantage encodes errors as JSON body keys
                if "Error Message" in data:
                    logger.warning("Alpha Vantage error for %s: %s", params, data["Error Message"])
                    return None
                if "Note" in data:
                    # Rate-limit notice — wait and retry
                    logger.warning("Alpha Vantage rate-limit notice. Waiting 60 s ...")
                    time.sleep(60)
                    continue
                if "Information" in data:
                    logger.warning("Alpha Vantage info message: %s", data["Information"])
                    return None

                time.sleep(self.delay)
                return data

            except requests.HTTPError as exc:
                last_exc = exc
                logger.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
                time.sleep(2 ** attempt)
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
                time.sleep(2 ** attempt)

        logger.error("All %d attempts failed for params %s", retries, params)
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def weekly_adjusted(self, symbol: str) -> dict | None:
        """
        Fetch the full weekly adjusted price/volume time series for *symbol*.

        Returns the parsed JSON dict on success, or None on API error / unknown symbol.

        Response structure (relevant keys):
        {
            "Meta Data": { "2. Symbol": "AAPL", ... },
            "Weekly Adjusted Time Series": {
                "2024-01-05": {
                    "1. open": "...",
                    "2. high": "...",
                    "3. low": "...",
                    "4. close": "...",
                    "5. adjusted close": "...",
                    "6. volume": "...",
                    "7. dividend amount": "..."
                },
                ...
            }
        }
        """
        params = {
            "function": "TIME_SERIES_WEEKLY_ADJUSTED",
            "symbol": symbol.upper(),
            "datatype": "json",
        }
        logger.debug("Fetching weekly adjusted prices for %s", symbol)
        return self._get(params)
