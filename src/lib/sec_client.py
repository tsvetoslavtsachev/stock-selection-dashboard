"""
SEC EDGAR client — fetches companyfacts and submissions for a given CIK.

Endpoints:
  https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json
  https://data.sec.gov/submissions/CIK{cik10}.json

SEC requires a descriptive User-Agent header per:
  https://www.sec.gov/developer
"""

from __future__ import annotations

import time
import logging
from typing import Any

import requests
import yaml

from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load User-Agent from config/settings.yml (falls back to a safe default)
# ---------------------------------------------------------------------------
_SETTINGS_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yml"


def _load_user_agent() -> str:
    # Try settings.yml first, fall back to settings.example.yml (used in CI)
    candidates = [_SETTINGS_PATH, _SETTINGS_PATH.parent / "settings.example.yml"]
    for path in candidates:
        try:
            with open(path) as f:
                cfg = yaml.safe_load(f)
            agent = cfg.get("api", {}).get("sec_user_agent") or cfg.get("sec", {}).get("user_agent")
            if agent:
                return agent
        except FileNotFoundError:
            continue
    return "StockDashboard contact@example.com"


class SECClient:
    """
    Thin wrapper around the public SEC EDGAR data API.

    Parameters
    ----------
    rate_limit_delay : float
        Seconds to wait between requests to respect SEC's rate-limit guidance
        (10 requests / second maximum; default 0.12 s ≈ ~8 req/s to be safe).
    """

    BASE_URL = "https://data.sec.gov"

    def __init__(self, rate_limit_delay: float = 0.12) -> None:
        self.delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": _load_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov",
            }
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pad_cik(cik: str | int) -> str:
        """Zero-pad CIK to 10 digits as required by SEC endpoints."""
        return str(int(cik)).zfill(10)

    def _get(self, url: str, retries: int = 3) -> Any:
        """GET with simple retry logic on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp.json()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    logger.warning("404 Not Found: %s", url)
                    return None
                last_exc = exc
                logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
                time.sleep(2 ** attempt)
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
                time.sleep(2 ** attempt)
        logger.error("All %d attempts failed for %s", retries, url)
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def companyfacts(self, cik: str | int) -> dict | None:
        """
        Fetch the full XBRL company facts JSON for a given CIK.

        Returns the parsed JSON dict, or None if the CIK is not found.

        Example URL:
            https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json
        """
        cik10 = self._pad_cik(cik)
        url = f"{self.BASE_URL}/api/xbrl/companyfacts/CIK{cik10}.json"
        logger.debug("Fetching companyfacts for CIK %s", cik10)
        return self._get(url)

    def submissions(self, cik: str | int) -> dict | None:
        """
        Fetch the submissions metadata JSON for a given CIK.

        Returns the parsed JSON dict, or None if the CIK is not found.

        Example URL:
            https://data.sec.gov/submissions/CIK0000320193.json
        """
        cik10 = self._pad_cik(cik)
        url = f"{self.BASE_URL}/submissions/CIK{cik10}.json"
        logger.debug("Fetching submissions for CIK %s", cik10)
        return self._get(url)
