"""
services/movers_service.py - Star stock movers detection and reason attribution.

Identifies top gainers/losers across US, HK, and CN markets
and attributes reasons from news sources.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

from providers.base import USER_AGENT

logger = logging.getLogger(__name__)

# Pre-defined watchlist of popular/star stocks per market
STAR_STOCKS = {
    'US': [
        {"symbol": "NVDA", "name": "NVIDIA"},
        {"symbol": "AAPL", "name": "Apple"},
        {"symbol": "MSFT", "name": "Microsoft"},
        {"symbol": "GOOGL", "name": "Google"},
        {"symbol": "AMZN", "name": "Amazon"},
        {"symbol": "META", "name": "Meta"},
        {"symbol": "TSLA", "name": "Tesla"},
        {"symbol": "TSM", "name": "TSMC"},
        {"symbol": "AVGO", "name": "Broadcom"},
        {"symbol": "AMD", "name": "AMD"},
        {"symbol": "NFLX", "name": "Netflix"},
        {"symbol": "CRM", "name": "Salesforce"},
        {"symbol": "COIN", "name": "Coinbase"},
        {"symbol": "PLTR", "name": "Palantir"},
        {"symbol": "MSTR", "name": "MicroStrategy"},
        {"symbol": "ARM", "name": "ARM Holdings"},
        {"symbol": "SMCI", "name": "Super Micro"},
        {"symbol": "SNOW", "name": "Snowflake"},
        {"symbol": "SHOP", "name": "Shopify"},
        {"symbol": "SQ", "name": "Block Inc"},
    ],
    'HK': [
        {"symbol": "9988.HK", "name": "阿里巴巴"},
        {"symbol": "0700.HK", "name": "腾讯"},
        {"symbol": "3690.HK", "name": "美团"},
        {"symbol": "9999.HK", "name": "网易"},
        {"symbol": "9618.HK", "name": "京东"},
        {"symbol": "1024.HK", "name": "快手"},
        {"symbol": "9888.HK", "name": "百度"},
        {"symbol": "0981.HK", "name": "中芯国际"},
        {"symbol": "2015.HK", "name": "理想汽车"},
        {"symbol": "9866.HK", "name": "蔚来"},
        {"symbol": "1810.HK", "name": "小米集团"},
        {"symbol": "9626.HK", "name": "哔哩哔哩"},
    ],
    'CN': [
        {"symbol": "600519.SS", "name": "贵州茅台"},
        {"symbol": "000858.SZ", "name": "五粮液"},
        {"symbol": "300750.SZ", "name": "宁德时代"},
        {"symbol": "601318.SS", "name": "中国平安"},
        {"symbol": "000001.SZ", "name": "平安银行"},
        {"symbol": "600036.SS", "name": "招商银行"},
        {"symbol": "002594.SZ", "name": "比亚迪"},
        {"symbol": "601012.SS", "name": "隆基绿能"},
        {"symbol": "688981.SS", "name": "中芯国际"},
        {"symbol": "603259.SS", "name": "药明康德"},
    ],
}

YAHOO_CHART = "https://query2.finance.yahoo.com/v8/finance/chart"


class MoversService:
    """Detects star stock movers and attributes reasons."""

    def __init__(self, config: dict):
        self.config = config
        brief_cfg = config.get('daily_brief', {})
        movers_cfg = brief_cfg.get('movers', {})

        self.markets = movers_cfg.get('markets', ['US', 'HK', 'CN'])
        self.top_n = movers_cfg.get('top_n', 10)
        self.min_change = movers_cfg.get('min_change_pct', 3.0)
        self._session = None
        self._crumb = None

    def _get_session(self):
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
        except Exception:
            self._crumb = None
        return self._session, self._crumb

    def detect_movers(self) -> dict:
        """
        Detect top gainers and losers from star stock lists.
        Returns: {gainers: [...], losers: [...], status, timestamp}
        """
        all_stocks = []

        session, crumb = self._get_session()

        for market in self.markets:
            stocks = STAR_STOCKS.get(market, [])
            for stock in stocks:
                try:
                    data = self._fetch_stock_data(stock['symbol'], session, crumb)
                    if data and data.get('change_pct') is not None:
                        data['name'] = stock['name']
                        data['market'] = market
                        all_stocks.append(data)
                except Exception as e:
                    logger.debug(f"Failed to fetch {stock['symbol']}: {e}")
                time.sleep(0.1)  # Rate limiting

        if not all_stocks:
            return {
                'gainers': [],
                'losers': [],
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
            }

        # Sort by change
        all_stocks.sort(key=lambda x: x.get('change_pct', 0), reverse=True)

        # Top gainers (significant moves only)
        gainers = [s for s in all_stocks if (s.get('change_pct', 0) or 0) >= self.min_change][:self.top_n]

        # Top losers
        losers_pool = [s for s in reversed(all_stocks) if (s.get('change_pct', 0) or 0) <= -self.min_change]
        losers = losers_pool[:self.top_n]

        # Attribute reasons from news
        for stock in gainers + losers:
            stock['reason'] = self._find_reason(stock)

        return {
            'gainers': gainers,
            'losers': losers,
            'total_scanned': len(all_stocks),
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
        }

    def _fetch_stock_data(self, symbol: str, session, crumb) -> Optional[dict]:
        """Fetch latest price and change for a single stock."""
        url = f"{YAHOO_CHART}/{symbol}"
        params = {"range": "5d", "interval": "1d", "includePrePost": "false"}
        if crumb:
            params["crumb"] = crumb

        try:
            resp = session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None
        except Exception:
            return None

        try:
            data = resp.json()
            result = data["chart"]["result"][0]
            meta = result.get("meta", {})
            quote = result["indicators"]["quote"][0]
            closes = quote.get("close", [])
            volumes = quote.get("volume", [])

            # Find latest valid close
            latest_close = None
            prev_close = None
            latest_vol = None

            for i in range(len(closes) - 1, -1, -1):
                if closes[i] is not None:
                    if latest_close is None:
                        latest_close = closes[i]
                        latest_vol = volumes[i] if i < len(volumes) else None
                    elif prev_close is None:
                        prev_close = closes[i]
                        break

            if prev_close is None:
                prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")

            if latest_close is None or prev_close is None or prev_close == 0:
                return None

            change_pct = ((latest_close - prev_close) / prev_close) * 100

            return {
                'symbol': symbol,
                'price': round(latest_close, 2),
                'change_pct': round(change_pct, 2),
                'change_abs': round(latest_close - prev_close, 2),
                'volume': latest_vol,
                'prev_close': round(prev_close, 2),
            }
        except Exception as e:
            logger.debug(f"Parse error for {symbol}: {e}")
            return None

    def _find_reason(self, stock: dict) -> dict:
        """Find reason for stock movement from news."""
        try:
            from providers.news_provider import search_news_for_stock
            articles = search_news_for_stock(stock['name'], stock['symbol'])

            if articles:
                best = articles[0]
                return {
                    'text': best.get('title', '暂无可靠原因'),
                    'source': best.get('source', ''),
                    'url': best.get('url', ''),
                    'confidence': 'high' if len(articles) > 1 else 'medium',
                }
        except Exception as e:
            logger.debug(f"News search failed for {stock['symbol']}: {e}")

        return {
            'text': '暂无可靠原因',
            'source': '',
            'url': '',
            'confidence': 'none',
        }
