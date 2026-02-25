"""
services/market_service.py - Market data business logic.

Processes raw market provider data into display-ready format with
timezone handling, trading status, and formatting.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class MarketService:
    """Processes market index data for display."""

    def __init__(self, config: dict):
        self.config = config

    def process(self, raw_market_data: dict) -> dict:
        """
        Process raw market data into display-ready format.

        Returns: {
            status: str,
            indices: [...],
            summary: str,
            update_time: str,
        }
        """
        indices = raw_market_data.get('data', [])
        processed = []

        for idx in indices:
            processed.append(self._process_index(idx))

        # Overall status
        ok_count = sum(1 for p in processed if p['price'] is not None)
        if ok_count == len(processed):
            status = 'ok'
            status_text = '数据正常'
        elif ok_count > 0:
            status = 'partial'
            status_text = f'部分成功 ({ok_count}/{len(processed)})'
        else:
            status = 'error'
            status_text = '数据获取失败'

        # Summary
        changes = [p['change_pct'] for p in processed if p['change_pct'] is not None]
        if changes:
            up_count = sum(1 for c in changes if c > 0)
            down_count = sum(1 for c in changes if c < 0)
            avg_chg = sum(changes) / len(changes)
            if avg_chg > 1:
                summary = f"全球市场偏强，{up_count}涨{down_count}跌"
            elif avg_chg < -1:
                summary = f"全球市场偏弱，{up_count}涨{down_count}跌"
            else:
                summary = f"市场涨跌互现，{up_count}涨{down_count}跌"
        else:
            summary = "暂无数据"

        return {
            'status': status,
            'status_text': status_text,
            'indices': processed,
            'summary': summary,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M'),
        }

    def _process_index(self, idx: dict) -> dict:
        """Process a single index entry."""
        change_pct = idx.get('change_pct')
        price = idx.get('price')

        # Format price
        if price is not None:
            if price > 10000:
                price_display = f"{price:,.0f}"
            elif price > 100:
                price_display = f"{price:,.1f}"
            else:
                price_display = f"{price:,.2f}"
        else:
            price_display = "N/A"

        # Format change
        if change_pct is not None:
            change_display = f"{'+' if change_pct > 0 else ''}{change_pct:.2f}%"
            if change_pct > 0:
                change_color = '#22c55e'  # green
                change_emoji = '📈'
            elif change_pct < 0:
                change_color = '#ef4444'  # red
                change_emoji = '📉'
            else:
                change_color = '#94a3b8'  # gray
                change_emoji = '➡️'
        else:
            change_display = "N/A"
            change_color = '#94a3b8'
            change_emoji = '❓'

        # Day range
        day_range = None
        if idx.get('day_low') and idx.get('day_high'):
            day_range = f"{idx['day_low']:,.1f} - {idx['day_high']:,.1f}"

        # Trading status styling
        status = idx.get('trading_status', '未知')
        status_color = {
            '盘中': '#3b82f6',   # blue (active)
            '盘前': '#8b5cf6',   # purple
            '收盘': '#64748b',   # gray
            '休市': '#475569',   # dark gray
            '24h': '#22c55e',    # green
        }.get(status, '#94a3b8')

        return {
            'symbol': idx.get('symbol', ''),
            'name': idx.get('name', ''),
            'name_en': idx.get('name_en', ''),
            'market': idx.get('market', ''),
            'price': price,
            'price_display': price_display,
            'change_pct': change_pct,
            'change_abs': idx.get('change_abs'),
            'change_display': change_display,
            'change_color': change_color,
            'change_emoji': change_emoji,
            'prev_close': idx.get('prev_close'),
            'day_high': idx.get('day_high'),
            'day_low': idx.get('day_low'),
            'day_range': day_range,
            'volume': idx.get('volume'),
            'trading_status': status,
            'status_color': status_color,
            'data_time': idx.get('data_time'),
            'currency': idx.get('currency', ''),
        }
