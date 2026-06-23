"""Async HTTP client for Riot Games API with rate limiting and retry support.

Uses httpx for async HTTP requests, tenacity for retry logic, and asyncio.Semaphore
for concurrency control.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


RIOT_API_KEY: str = os.environ["RIOT_API_KEY"]  # Raises KeyError if not set


class RateLimitError(Exception):
    """Raised when Riot API returns 429 Too Many Requests."""

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        """Initialize RateLimitError.

        Args:
            retry_after_seconds: Optional seconds to wait before retrying.

        """
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"Rate limited. Retry after {retry_after_seconds}s" if retry_after_seconds else "Rate limited"
        )


def _is_retryable_status_code(exception: BaseException) -> bool:
    """Check if the exception indicates a retryable HTTP error."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in (500, 502, 503, 504)
    if isinstance(exception, httpx.TransportError):
        return True
    return False


_retry_decorator = retry(
    retry=retry_if_exception(_is_retryable_status_code),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    reraise=True,
)


class RiotClient:
    """Async HTTP client for Riot Games API.

    Provides rate limiting via semaphore, automatic retry on 429/5xx errors,
    and structured API request methods.

    Args:
        platform: Platform ID (e.g., "vn2", "kr", "euw1").
        max_concurrent: Maximum concurrent requests (default 10).

    """

    __slots__ = ("_client", "_semaphore", "platform")

    def __init__(
        self,
        platform: str,
        *,
        max_concurrent: int = 10,
    ) -> None:
        """Initialize the RiotClient.

        Args:
            platform: Platform ID (e.g., "vn2", "kr", "euw1").
            max_concurrent: Maximum concurrent requests (default 10).

        """
        self.platform = platform
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._client: httpx.AsyncClient | None = None

    def _get_base_url(self) -> str:
        """Get the base URL for the platform."""
        return f"https://{self.platform}.api.riotgames.com"

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Lazily initialize the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._get_base_url(),
                headers={"X-RIOT-API-KEY": RIOT_API_KEY},
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> RiotClient:
        """Enter async context manager."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Exit async context manager."""
        await self.close()

    @staticmethod
    def _make_request_with_semaphore(
        semaphore: asyncio.Semaphore,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Callable[[], Awaitable[httpx.Response]]:
        """Create a request function that respects semaphore limits."""

        async def _request() -> httpx.Response:
            async with semaphore:
                response = await client.request(method, path, **kwargs)
                if response.status_code == 429:
                    retry_after = None
                    retry_after_header = response.headers.get("Retry-After")
                    if retry_after_header is not None:
                        try:
                            retry_after = float(retry_after_header)
                        except ValueError:
                            pass
                    raise RateLimitError(retry_after_seconds=retry_after)
                response.raise_for_status()
                return response

        return _request

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute an HTTP request with retry logic and concurrency control.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: API endpoint path.
            **kwargs: Additional arguments passed to httpx request.

        Returns:
            httpx.Response: The HTTP response.

        Raises:
            httpx.HTTPStatusError: On non-retryable HTTP errors.

        """
        client = await self._ensure_client()
        request_fn = self._make_request_with_semaphore(
            self._semaphore, client, method, path, **kwargs
        )

        decorated_request = _retry_decorator(request_fn)
        return await decorated_request()

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        """Execute a GET request.

        Args:
            path: API endpoint path.
            **kwargs: Additional query parameters.

        Returns:
            httpx.Response: The HTTP response.

        """
        return await self._request_with_retry("GET", path, **kwargs)

    async def get_json(self, path: str, **kwargs: Any) -> Any:
        """Execute a GET request and return parsed JSON.

        Args:
            path: API endpoint path.
            **kwargs: Additional query parameters.

        Returns:
            Parsed JSON response data.

        """
        response = await self.get(path, **kwargs)
        return response.json()
