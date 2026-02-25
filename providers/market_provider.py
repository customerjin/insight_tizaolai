"""
providers/market_provider.py - Market data provider using Yahoo Finance.

Fetches real-time/delayed quotes for major indices with timezone-aware
trading status detection (open/closed/holiday).
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from .base import BaseProvider, http_get, USER_AGENT

logger = logging.getLogger(__name__)

# Yahoo Finance v8 chart API
YAHOO_CHART = "https://query2.finance.yahoo.com/v8/finance/chart"
YAHOO_QUOTE = "https://query2.finance.yahoo.com/v7/finance/quote"

# Default index definitions
DEFAULT_INDICES = [
    {
        "symbol": "^NDX",
        "name": "纳斯达克100",
        "name_en": "Nasdaq 100",
        "market": "US",
        "timezone": "US/Eastern",
        "trading_hours": {"open": "09:30", "close": "16:00"},
        "currency": "USD",
    },
    {
        "symbol": "000300.SS",
        "name": "沪深300",
        "name_en": "CSI 300",
        "market": "CN",
        "timezone": "Asia/Shanghai",
        "trading_hours": {"open": "09:30", "close": "15:00"},
        "currency": "CNY",
    },
    {
        "symbol": "^HSTECH",
        "name": "恒生科技",
        "name_en": "Hang Seng Tech",
        "market": "HK",
        "timezone": "Asia/Hong_Kong",
        "trading_hours": {"open": "09:30", "close": "16:00"},
        "currency": "HKD",
    },
    {
        "symbol": "000001.SS",
        "name": "上证指数",
        "name_en": "SSE Composite",
        "market": "CN",
        "timezone": "Asia/Shanghai",
        "trading_hours": {"open": "09:30", "close": "15:00"},
        "currency": "CNY",
    },
    {
        "symbol": "BTC-USD",
        "name": "比特币",
        "name_en": "Bitcoin",
        "market": "CRYPTO",
        "timezone": "UTC",
        "trading_hours": {"open": "00:00", "close": "23:59"},
        "currency": "USD",
    },
]


class MarketProvider(BaseProvider):
    """Provides real-time market index data via Yahoo Finance."""

    def __init__(self, config: dict):
        brief_cfg = config.get('daily_brief', {})
        cache_ttl = brief_cfg.get('cache', {}).get('market_ttl', 300)

        cache_dir = config.get('cache', {}).get('dir', 'cache')
        super().__init__(config, cache_dir=f"{cache_dir}/brief/market", cache_ttl=cache_ttl)

        # Build index list from config or use defaults
        configured = brief_cfg.get('market_indices', [])
        if configured:
            self.indices = configured
        else:
            self.indices = DEFAULT_INDICES

        self._session = None
        self._crumb = None

    def _get_session(self):
        """Get Yahoo Finance session with crumb."""
        if self._session is not None:
            return self._session, self._crumb

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        try:
            self._session.get("https://fc.yahoo.com", timeout=10)
            crumb_resp = self._session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10
            )
            if crumb_resp.status_code == 200 and crumb_resp.text:
                self._crumb = crumb_resp.text
                logger.info("Yahoo session: crumb OK")
        except Exception as e:
            logger.warning(f"Yahoo crumb failed: {e}")
            self._crumb = None

        return self._session, self._crumb

    def fetch_all_indices(self) -> dict:
        """
        Fetch data for all configured indices.
        Returns: {status, data: [...], timestamp}
        """
        results = []
        session, crumb = self._get_session()

        for idx_cfg in self.indices:
            try:
                data = self._fetch_single_index(idx_cfg, session, crumb)
                results.append(data)
            except Exception as e:
                logger.warning(f"Failed to fetch {idx_cfg['symbol']}: {e}")
                results.append({
                    **idx_cfg,
                    'price': None,
                    'change_pct': None,
                    'change_abs': None,
                    'prev_close': None,
                    'day_high': None,
                    'day_low': None,
                    'volume': None,
                    'trading_status': 'error',
                    'data_time': None,
                    'error': str(e),
                })

        ok_count = sum(1 for r in results if r.get('price') is not None)
        return {
            'status': 'ok' if ok_count == len(results) else ('partial' if ok_count > 0 else 'error'),
            'data': results,
            'ok_count': ok_count,
            'total': len(results),
            'timestamp': datetime.now().isoformat(),
        }

    def _fetch_single_index(self, idx_cfg: dict, session, crumb) -> dict:
        """Fetch a single index from Yahoo Finance chart API."""
        symbol = idx_cfg['symbol']
        url = f"{YAHOO_CHART}/{symbol}"
        params = {
            "range": "5d",
            "interval": "1d",
            "includePrePost": "false",
        }
        if crumb:
            params["crumb"] = crumb

        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                fallback_url = url.replace("query2.", "query1.")
                resp = requests.get(fallback_url, params={
                    "range": "5d", "interval": "1d", "includePrePost": "false"
                }, headers={"User-Agent": USER_AGENT}, timeout=15)
        except Exception:
            # Try without session
            resp = requests.get(url, params={
                "range": "5d", "interval": "1d", "includePrePost": "false"
            }, headers={"User-Agent": USER_AGENT}, timeout=15)

        data = resp.json()
        result = data["chart"]["result"][0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]

        closes = quote.get("close", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        volumes = quote.get("volume", [])

        # Get latest valid data
        latest_close = None
        prev_close = None
        latest_high = None
        latest_low = None
        latest_volume = None
        latest_ts = None

        # Walk backwards to find latest valid close
        for i in range(len(closes) - 1, -1, -1):
            if closes[i] is not None:
                if latest_close is None:
                    latest_close = closes[i]
                    latest_high = highs[i] if i < len(highs) else None
                    latest_low = lows[i] if i < len(lows) else None
                    latest_volume = volumes[i] if i < len(volumes) else None
                    latest_ts = timestamps[i] if i < len(timestamps) else None
                elif prev_close is None:
                    prev_close = closes[i]
                    break

        # Use meta for additional info
        if prev_close is None:
            prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")

        # Calculate change
        change_pct = None
        change_abs = None
        if latest_close is not None and prev_close is not None and prev_close != 0:
            change_abs = latest_close - prev_close
            change_pct = (change_abs / prev_close) * 100

        # Determine trading status
        trading_status = self._get_trading_status(idx_cfg)

        # Format data timestamp
        data_time = None
        if latest_ts:
            data_time = datetime.fromtimestamp(latest_ts, tz=ZoneInfo('UTC')).isoformat()

        return {
            **idx_cfg,
            'price': round(latest_close, 2) if latest_close else None,
            'change_pct': round(change_pct, 2) if change_pct is not None else None,
            'change_abs': round(change_abs, 2) if change_abs is not None else None,
            'prev_close': round(prev_close, 2) if prev_close else None,
            'day_high': round(latest_high, 2) if latest_high else None,
            'day_low': round(latest_low, 2) if latest_low else None,
            'volume': latest_volume,
            'trading_status': trading_status,
            'data_time': data_time,
        }

    def _get_trading_status(self, idx_cfg: dict) -> str:
        """Determine if market is currently open, closed, or holiday."""
        market = idx_cfg.get('market', '')

        if market == 'CRYPTO':
            return '24h'

        tz_str = idx_cfg.get('timezone', 'UTC')
        tz = ZoneInfo(tz_str)
        now = datetime.now(tz)

        # Weekend check
        if now.weekday() >= 5:
            return '休市'

        hours = idx_cfg.get('trading_hours', {})
        open_str = hours.get('open', '09:30')
        close_str = hours.get('close', '16:00')

        open_h, open_m = map(int, open_str.split(':'))
        close_h, close_m = map(int, close_str.split(':'))

        open_time = now.replace(hour=open_h, minute=open_m, second=0)
        close_time = now.replace(hour=close_h, minute=close_m, second=0)

        if now < open_time:
            return '盘前'
        elif now > close_time:
            return '收盘'
        else:
            return '盘中'

    # BaseProvider interface (for individual key-based fetch)
    def _fetch_impl(self, key: str, **kwargs) -> dict:
        return self.fetch_all_indices()

    def _fallback_impl(self, key: str, **kwargs) -> dict:
        """Return empty data structure as fallback."""
        return {
            'status': 'fallback',
            'data': [{**idx, 'price': None, 'change_pct': None, 'trading_status': 'unavailable'}
                     for idx in self.indices],
            'timestamp': datetime.now().isoformat(),
        }
