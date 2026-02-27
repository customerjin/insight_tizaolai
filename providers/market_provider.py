"""
providers/market_provider.py - Market data provider using yfinance.

Uses the yfinance library which handles Yahoo Finance session/crumb/cookies
automatically, much more reliable than raw API calls.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from .base import BaseProvider

logger = logging.getLogger(__name__)

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
    """Provides real-time market index data via yfinance."""

    def __init__(self, config: dict):
        brief_cfg = config.get('daily_brief', {})
        cache_ttl = brief_cfg.get('cache', {}).get('market_ttl', 300)
        cache_dir = config.get('cache', {}).get('dir', 'cache')
        super().__init__(config, cache_dir=f"{cache_dir}/brief/market", cache_ttl=cache_ttl)

        configured = brief_cfg.get('market_indices', [])
        self.indices = configured if configured else DEFAULT_INDICES

    def fetch_all_indices(self) -> dict:
        """Fetch data for all configured indices using yfinance."""
        try:
            import yfinance as yf
        except ImportError:
            error_msg = "yfinance 未安装，请运行: pip3 install yfinance --break-system-packages"
            logger.error(error_msg)
            return self._make_error_result(error_msg)

        results = []
        symbols = [idx['symbol'] for idx in self.indices]

        try:
            # Batch download - more efficient than one-by-one
            tickers = yf.Tickers(' '.join(symbols))

            for idx_cfg in self.indices:
                symbol = idx_cfg['symbol']
                try:
                    ticker = tickers.tickers.get(symbol)
                    if ticker is None:
                        # Try fetching individually as fallback
                        ticker = yf.Ticker(symbol)

                    data = self._extract_ticker_data(ticker, idx_cfg)
                    results.append(data)
                except Exception as e:
                    logger.warning(f"Failed to fetch {symbol}: {e}")
                    results.append(self._make_error_entry(idx_cfg, str(e)))

        except Exception as e:
            # Batch failed, try one by one
            logger.warning(f"Batch download failed: {e}, trying individually...")
            for idx_cfg in self.indices:
                try:
                    ticker = yf.Ticker(idx_cfg['symbol'])
                    data = self._extract_ticker_data(ticker, idx_cfg)
                    results.append(data)
                except Exception as e2:
                    logger.warning(f"Individual fetch failed {idx_cfg['symbol']}: {e2}")
                    results.append(self._make_error_entry(idx_cfg, str(e2)))

        ok_count = sum(1 for r in results if r.get('price') is not None)
        return {
            'status': 'ok' if ok_count == len(results) else ('partial' if ok_count > 0 else 'error'),
            'data': results,
            'ok_count': ok_count,
            'total': len(results),
            'timestamp': datetime.now().isoformat(),
        }

    def _extract_ticker_data(self, ticker, idx_cfg: dict) -> dict:
        """Extract price data from a yfinance Ticker object."""
        symbol = idx_cfg['symbol']

        # Try fast_info first, then info
        try:
            fi = ticker.fast_info
            price = fi.get('lastPrice') or fi.get('last_price')
            prev_close = fi.get('previousClose') or fi.get('previous_close') or fi.get('regularMarketPreviousClose')
            day_high = fi.get('dayHigh') or fi.get('day_high')
            day_low = fi.get('dayLow') or fi.get('day_low')
        except Exception:
            price = None
            prev_close = None
            day_high = None
            day_low = None

        # Fallback: use history if fast_info failed
        if price is None:
            try:
                hist = ticker.history(period="5d")
                if not hist.empty:
                    price = float(hist['Close'].iloc[-1])
                    if len(hist) >= 2:
                        prev_close = float(hist['Close'].iloc[-2])
                    day_high = float(hist['High'].iloc[-1])
                    day_low = float(hist['Low'].iloc[-1])
            except Exception as e:
                logger.warning(f"History fallback failed for {symbol}: {e}")

        if price is None:
            return self._make_error_entry(idx_cfg, f"No price data returned for {symbol}")

        # Calculate change
        change_pct = None
        change_abs = None
        if prev_close and prev_close != 0:
            change_abs = round(price - prev_close, 2)
            change_pct = round((change_abs / prev_close) * 100, 2)

        trading_status = self._get_trading_status(idx_cfg)

        return {
            **idx_cfg,
            'price': round(price, 2),
            'change_pct': change_pct,
            'change_abs': change_abs,
            'prev_close': round(prev_close, 2) if prev_close else None,
            'day_high': round(day_high, 2) if day_high else None,
            'day_low': round(day_low, 2) if day_low else None,
            'volume': None,
            'trading_status': trading_status,
            'data_time': datetime.now().isoformat(),
        }

    def _make_error_entry(self, idx_cfg: dict, error: str) -> dict:
        return {
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
            'error': error,
        }

    def _make_error_result(self, error_msg: str) -> dict:
        return {
            'status': 'error',
            'data': [self._make_error_entry(idx, error_msg) for idx in self.indices],
            'ok_count': 0,
            'total': len(self.indices),
            'timestamp': datetime.now().isoformat(),
        }

    def _get_trading_status(self, idx_cfg: dict) -> str:
        """Determine if market is currently open, closed, or holiday."""
        market = idx_cfg.get('market', '')
        if market == 'CRYPTO':
            return '24h'

        tz = ZoneInfo(idx_cfg.get('timezone', 'UTC'))
        now = datetime.now(tz)

        if now.weekday() >= 5:
            return '休市'

        hours = idx_cfg.get('trading_hours', {})
        open_h, open_m = map(int, hours.get('open', '09:30').split(':'))
        close_h, close_m = map(int, hours.get('close', '16:00').split(':'))
        open_time = now.replace(hour=open_h, minute=open_m, second=0)
        close_time = now.replace(hour=close_h, minute=close_m, second=0)

        if now < open_time:
            return '盘前'
        elif now > close_time:
            return '收盘'
        else:
            return '盘中'

    def _fetch_impl(self, key: str, **kwargs) -> dict:
        return self.fetch_all_indices()

    def _fallback_impl(self, key: str, **kwargs) -> dict:
        return self._make_error_result("Fallback: all sources exhausted")
