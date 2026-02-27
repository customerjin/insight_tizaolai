"""
services/movers_service.py - Star stock movers detection and reason attribution.

Uses yfinance for reliable data fetching across US, HK, and CN markets.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

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


class MoversService:
    """Detects star stock movers and attributes reasons."""

    def __init__(self, config: dict):
        self.config = config
        brief_cfg = config.get('daily_brief', {})
        movers_cfg = brief_cfg.get('movers', {})

        self.markets = movers_cfg.get('markets', ['US', 'HK', 'CN'])
        self.top_n = movers_cfg.get('top_n', 10)
        self.min_change = movers_cfg.get('min_change_pct', 3.0)

    def detect_movers(self) -> dict:
        """Detect top gainers and losers from star stock lists."""
        try:
            import yfinance as yf
        except ImportError:
            logger.error("yfinance not installed")
            return {
                'gainers': [], 'losers': [],
                'status': 'error',
                'error': 'yfinance 未安装，请运行: pip3 install yfinance --break-system-packages',
                'timestamp': datetime.now().isoformat(),
            }

        all_stocks = []

        for market in self.markets:
            stocks = STAR_STOCKS.get(market, [])
            symbols = [s['symbol'] for s in stocks]
            name_map = {s['symbol']: s['name'] for s in stocks}

            try:
                # Batch download per market
                data = yf.download(symbols, period="5d", interval="1d",
                                   group_by='ticker', progress=False, threads=True)

                for stock_cfg in stocks:
                    sym = stock_cfg['symbol']
                    try:
                        if len(symbols) == 1:
                            df = data
                        else:
                            df = data[sym] if sym in data.columns.get_level_values(0) else None

                        if df is None or df.empty:
                            continue

                        closes = df['Close'].dropna()
                        if len(closes) < 2:
                            continue

                        latest = float(closes.iloc[-1])
                        prev = float(closes.iloc[-2])
                        if prev == 0:
                            continue

                        change_pct = round(((latest - prev) / prev) * 100, 2)

                        all_stocks.append({
                            'symbol': sym,
                            'name': name_map[sym],
                            'market': market,
                            'price': round(latest, 2),
                            'change_pct': change_pct,
                            'change_abs': round(latest - prev, 2),
                            'prev_close': round(prev, 2),
                        })
                    except Exception as e:
                        logger.debug(f"Failed to parse {sym}: {e}")

            except Exception as e:
                logger.warning(f"Batch download failed for {market}: {e}")
                # Try individual downloads
                for stock_cfg in stocks:
                    try:
                        ticker = yf.Ticker(stock_cfg['symbol'])
                        hist = ticker.history(period="5d")
                        if hist.empty or len(hist) < 2:
                            continue
                        latest = float(hist['Close'].iloc[-1])
                        prev = float(hist['Close'].iloc[-2])
                        if prev == 0:
                            continue
                        change_pct = round(((latest - prev) / prev) * 100, 2)
                        all_stocks.append({
                            'symbol': stock_cfg['symbol'],
                            'name': stock_cfg['name'],
                            'market': market,
                            'price': round(latest, 2),
                            'change_pct': change_pct,
                            'change_abs': round(latest - prev, 2),
                            'prev_close': round(prev, 2),
                        })
                    except Exception as e2:
                        logger.debug(f"Individual failed {stock_cfg['symbol']}: {e2}")

        if not all_stocks:
            return {
                'gainers': [], 'losers': [],
                'status': 'error',
                'error': 'Yahoo Finance 无法获取任何股票数据，请检查网络连接',
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
