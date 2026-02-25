"""
services/news_service.py - News aggregation and Top 5 event selection.

Processes raw news from providers into deduplicated, ranked events
with impact assessment.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Market/sector impact keywords mapping
IMPACT_KEYWORDS = {
    '美股': ['wall street', 'nasdaq', 's&p', 'dow', 'nyse', 'us stock', 'us market'],
    'A股': ['china stock', 'shanghai', 'shenzhen', 'a-share', 'csi', 'chinese market'],
    '港股': ['hong kong', 'hang seng', 'hkex', 'hk stock'],
    '加密': ['bitcoin', 'crypto', 'btc', 'ethereum', 'eth', 'coinbase', 'binance'],
    '科技': ['tech', 'ai ', 'artificial intelligence', 'nvidia', 'apple', 'google', 'microsoft', 'semiconductor', 'chip'],
    '能源': ['oil', 'energy', 'opec', 'natural gas', 'solar', 'ev'],
    '金融': ['bank', 'fed', 'rate', 'bond', 'treasury', 'interest rate', 'inflation'],
    '地缘': ['tariff', 'trade war', 'sanction', 'geopolitical', 'conflict', 'war'],
}


class NewsService:
    """Processes and ranks financial news events."""

    def __init__(self, config: dict):
        self.config = config
        brief_cfg = config.get('daily_brief', {})
        self.top_n = brief_cfg.get('news', {}).get('top_n', 5)

    def process(self, raw_news: dict) -> dict:
        """
        Process raw news into Top 5 events with impact assessment.

        Returns: {
            top5: [{title, summary, impact_sectors, published, source, url, relevance_score}],
            all_articles: [...],
            status: str,
        }
        """
        articles = raw_news.get('articles', [])
        if not articles:
            return {
                'top5': [],
                'all_articles': [],
                'status': 'empty',
            }

        # Enrich each article with impact analysis
        for article in articles:
            article['impact_sectors'] = self._detect_impact_sectors(article)
            article['one_line_summary'] = self._generate_summary(article)

        # The articles should already be sorted by relevance from provider
        top5 = articles[:self.top_n]

        # Format for display
        formatted_top5 = []
        for i, article in enumerate(top5, 1):
            formatted_top5.append({
                'rank': i,
                'title': article.get('title', ''),
                'summary': article.get('one_line_summary', ''),
                'impact_sectors': article.get('impact_sectors', []),
                'published': self._format_time(article.get('published')),
                'published_raw': article.get('published'),
                'source': article.get('source', '未知来源'),
                'url': article.get('url', ''),
                'relevance_score': article.get('relevance_score', 0),
            })

        return {
            'top5': formatted_top5,
            'all_articles': articles[:20],
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
        }

    def _detect_impact_sectors(self, article: dict) -> List[str]:
        """Detect which market sectors an article impacts."""
        text = (article.get('title', '') + ' ' + article.get('summary', '')).lower()
        sectors = []

        for sector, keywords in IMPACT_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                sectors.append(sector)

        return sectors[:3] if sectors else ['综合']

    def _generate_summary(self, article: dict) -> str:
        """Generate a one-line summary from the article."""
        # Use the first sentence of the description, or truncated title
        summary = article.get('summary', '')
        if summary:
            # Take first sentence
            for sep in ['. ', '。', '! ', '？']:
                idx = summary.find(sep)
                if 0 < idx < 150:
                    return summary[:idx + 1].strip()
            if len(summary) > 100:
                return summary[:97] + '...'
            return summary

        title = article.get('title', '')
        if len(title) > 100:
            return title[:97] + '...'
        return title

    def _format_time(self, iso_time: Optional[str]) -> str:
        """Format ISO time to human-readable relative time."""
        if not iso_time:
            return '时间未知'
        try:
            dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            now = datetime.now()
            diff = now - dt

            hours = diff.total_seconds() / 3600
            if hours < 1:
                return f"{int(diff.total_seconds() / 60)}分钟前"
            elif hours < 24:
                return f"{int(hours)}小时前"
            elif hours < 48:
                return "昨天"
            else:
                return dt.strftime('%m-%d %H:%M')
        except (ValueError, TypeError):
            return str(iso_time)[:16]
