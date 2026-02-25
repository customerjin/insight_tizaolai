"""
providers/base.py - Provider base class with caching, rate limiting, retry, and fallback.

All data providers (market, news, analysis) inherit from this base.
Provides a consistent interface for fetching data with resilience built in.
"""

import time
import json
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Callable
from functools import wraps

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, max_calls: int = 10, period_seconds: int = 60):
        self.max_calls = max_calls
        self.period = period_seconds
        self._calls = []

    def wait_if_needed(self):
        now = time.time()
        self._calls = [t for t in self._calls if now - t < self.period]
        if len(self._calls) >= self.max_calls:
            sleep_time = self.period - (now - self._calls[0]) + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limit: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self._calls.append(time.time())


class JSONCache:
    """JSON-based cache with TTL support for provider results."""

    def __init__(self, cache_dir: str, default_ttl: int = 300):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.default_ttl = default_ttl

    def _key_path(self, key: str) -> Path:
        safe = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"brief_{safe}.json"

    def get(self, key: str, ttl: Optional[int] = None) -> Optional[Any]:
        path = self._key_path(key)
        if not path.exists():
            return None
        ttl = ttl or self.default_ttl
        age = time.time() - path.stat().st_mtime
        if age > ttl:
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.debug(f"Cache hit: {key} (age={age:.0f}s)")
            return data
        except Exception as e:
            logger.warning(f"Cache read error for {key}: {e}")
            return None

    def put(self, key: str, data: Any):
        path = self._key_path(key)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, default=str)
            logger.debug(f"Cached: {key}")
        except Exception as e:
            logger.warning(f"Cache write error for {key}: {e}")


def http_get(url: str, params: Optional[dict] = None, timeout: int = 30,
             max_retries: int = 3, session: Optional[requests.Session] = None) -> requests.Response:
    """HTTP GET with retries and exponential backoff."""
    headers = {"User-Agent": USER_AGENT}
    requester = session or requests
    for attempt in range(max_retries):
        try:
            resp = requester.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                wait = 2 * (2 ** attempt)
                logger.warning(f"Rate limited (429), waiting {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt+1})")
                if attempt < max_retries - 1:
                    time.sleep(1)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error: {e} (attempt {attempt+1})")
            if attempt < max_retries - 1:
                time.sleep(2)
    raise ConnectionError(f"Failed to fetch {url} after {max_retries} attempts")


class BaseProvider(ABC):
    """
    Abstract base class for all data providers.
    Subclasses implement _fetch_impl() and optionally _fallback_impl().
    """

    def __init__(self, config: dict, cache_dir: str = "cache/brief",
                 cache_ttl: int = 300, rate_limit: int = 30):
        self.config = config
        self.cache = JSONCache(cache_dir, default_ttl=cache_ttl)
        self.limiter = RateLimiter(max_calls=rate_limit, period_seconds=60)
        self.name = self.__class__.__name__

    def fetch(self, key: str, **kwargs) -> dict:
        """
        Fetch data with caching and fallback.
        Returns a dict with at least: {status, data, source, timestamp}
        """
        # Check cache
        cache_key = f"{self.name}:{key}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            cached['source'] = f"{self.name}:cache"
            return cached

        # Primary fetch
        try:
            self.limiter.wait_if_needed()
            result = self._fetch_impl(key, **kwargs)
            result.setdefault('status', 'ok')
            result.setdefault('source', self.name)
            result.setdefault('timestamp', datetime.now().isoformat())
            self.cache.put(cache_key, result)
            return result
        except Exception as e:
            logger.warning(f"{self.name} primary fetch failed for {key}: {e}")

        # Fallback
        try:
            result = self._fallback_impl(key, **kwargs)
            result.setdefault('status', 'fallback')
            result.setdefault('source', f"{self.name}:fallback")
            result.setdefault('timestamp', datetime.now().isoformat())
            self.cache.put(cache_key, result)
            return result
        except Exception as e2:
            logger.error(f"{self.name} fallback also failed for {key}: {e2}")

        return {
            'status': 'error',
            'data': None,
            'source': self.name,
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
        }

    @abstractmethod
    def _fetch_impl(self, key: str, **kwargs) -> dict:
        """Primary fetch implementation. Must return dict with 'data' key."""
        raise NotImplementedError

    def _fallback_impl(self, key: str, **kwargs) -> dict:
        """Fallback fetch implementation. Override in subclass if needed."""
        raise NotImplementedError(f"No fallback for {self.name}")
