"""
Session managers for NSE and BSE API access.

NSE requires cookie-based authentication via homepage visit.
BSE is more lenient but still benefits from proper headers.
"""

import asyncio
import random
import time
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import settings
from config.logging_config import get_logger

logger = get_logger(__name__)

# User agents to rotate for avoiding blocks
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


class NSESession:
    """
    Manages authenticated sessions with NSE India website.

    NSE blocks direct API access without cookies. You must:
    1. Hit the homepage to get cookies
    2. Use those cookies in all subsequent API calls
    3. Rotate User-Agent
    4. Respect rate limits (2-3 req/sec max)
    """

    BASE_URL = "https://www.nseindia.com"
    API_BASE = "https://www.nseindia.com/api"

    def __init__(self, rate_limit: Optional[float] = None):
        """
        Initialize NSE session.

        Args:
            rate_limit: Minimum seconds between requests. Defaults to settings value.
        """
        self.rate_limit = rate_limit or settings.nse_rate_limit
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time: float = 0
        self._cookies_refreshed_at: float = 0
        self._cookie_refresh_interval = 300  # Refresh cookies every 5 minutes

    async def __aenter__(self) -> "NSESession":
        """Enter async context manager."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.scrape_timeout),
            follow_redirects=True,
            headers={"User-Agent": random.choice(USER_AGENTS)},
        )
        await self._refresh_cookies()
        return self

    async def __aexit__(self, *args) -> None:
        """Exit async context manager."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _refresh_cookies(self) -> None:
        """Refresh session cookies by hitting the homepage."""
        if self._client is None:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")

        try:
            await self._client.get(
                self.BASE_URL,
                headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                },
            )
            self._cookies_refreshed_at = time.time()
            logger.debug("NSE cookies refreshed successfully")
        except Exception as e:
            logger.warning(f"Failed to refresh NSE cookies: {e}")
            raise

    async def _throttle(self) -> None:
        """Enforce rate limit between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            await asyncio.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    async def _maybe_refresh_cookies(self) -> None:
        """Refresh cookies if they're stale."""
        if time.time() - self._cookies_refreshed_at > self._cookie_refresh_interval:
            await self._refresh_cookies()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    )
    async def get(
        self,
        url: str,
        params: Optional[dict] = None,
        raw_response: bool = False,
    ) -> Any:
        """
        Make a rate-limited, retried GET request to NSE.

        Args:
            url: Full URL or API endpoint path
            params: Query parameters
            raw_response: If True, return raw response text instead of JSON

        Returns:
            Parsed JSON response or raw text
        """
        if self._client is None:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")

        await self._throttle()
        await self._maybe_refresh_cookies()

        # Build full URL if path provided
        if not url.startswith("http"):
            url = f"{self.API_BASE}/{url.lstrip('/')}"

        headers = {
            "Referer": self.BASE_URL,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "X-Requested-With": "XMLHttpRequest",
        }

        try:
            resp = await self._client.get(url, params=params, headers=headers)

            # Handle cookie expiration
            if resp.status_code == 401:
                logger.info("NSE cookies expired, refreshing...")
                await self._refresh_cookies()
                resp = await self._client.get(url, params=params, headers=headers)

            resp.raise_for_status()

            if raw_response:
                return resp.text

            try:
                return resp.json()
            except Exception:
                return resp.text

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching {url}: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise


class BSESession:
    """
    Manages sessions with BSE India API.

    BSE is more lenient than NSE - no mandatory cookie refresh,
    but still benefits from proper headers and rate limiting.
    """

    BASE_URL = "https://www.bseindia.com"
    API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"

    def __init__(self, rate_limit: Optional[float] = None):
        """
        Initialize BSE session.

        Args:
            rate_limit: Minimum seconds between requests. Defaults to settings value.
        """
        self.rate_limit = rate_limit or settings.bse_rate_limit
        self._client: Optional[httpx.AsyncClient] = None
        self._last_request_time: float = 0

    async def __aenter__(self) -> "BSESession":
        """Enter async context manager."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.scrape_timeout),
            follow_redirects=True,
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": self.BASE_URL,
            },
        )
        return self

    async def __aexit__(self, *args) -> None:
        """Exit async context manager."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _throttle(self) -> None:
        """Enforce rate limit between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            await asyncio.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    )
    async def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        raw_response: bool = False,
    ) -> Any:
        """
        Make a rate-limited, retried GET request to BSE API.

        Args:
            endpoint: API endpoint path (e.g., "ListofScripData/w")
            params: Query parameters
            raw_response: If True, return raw response text

        Returns:
            Parsed JSON response or raw text
        """
        if self._client is None:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")

        await self._throttle()

        url = f"{self.API_BASE}/{endpoint.lstrip('/')}"

        try:
            resp = await self._client.get(url, params=params)
            resp.raise_for_status()

            if raw_response:
                return resp.text

            try:
                return resp.json()
            except Exception:
                return resp.text

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching {url}: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise

    async def get_page(self, url: str) -> str:
        """
        Fetch a BSE web page (HTML).

        Args:
            url: Full URL to fetch

        Returns:
            HTML content
        """
        if self._client is None:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")

        await self._throttle()

        try:
            resp = await self._client.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.error(f"Error fetching page {url}: {e}")
            raise
