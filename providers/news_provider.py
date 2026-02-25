"""
providers/news_provider.py - News aggregation provider.

Multi-source news fetching with dedup and relevance scoring.
Primary: Google News RSS (free, no API key)
Backup: Finnhub (free tier, needs key)
"""

import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from difflib import SequenceMatcher
from urllib.parse import quote_plus

from .base import BaseProvider, http_get

logger = logging.getLogger(__name__)

# Google News RSS endpoints
GOOGLE_NEWS_RSS = "https://news.google.com/rss"
GOOGLE_NEWS_SEARCH = "https://news.google.com/rss/search"

# Topic IDs for Google News
TOPICS = {
    'business': 'CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB',
    'technology': 'CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRtTnpJU0FtVnVHZ0pWVXlnQVAB',
}

# Search keywords for financial news
FINANCE_KEYWORDS = [
    "stock market today",
    "Wall Street",
    "Federal Reserve",
    "cryptocurrency bitcoin",
    "tech stocks earnings",
    "IPO acquisition merger",
    "China economy A-shares",
    "Hong Kong stocks",
]

# High-authority news sources
HIGH_AUTHORITY_SOURCES = {
    'Reuters', 'Bloomberg', 'CNBC', 'The Wall Street Journal', 'Financial Times',
    'MarketWatch', 'Yahoo Finance', 'Barron\'s', 'The Economist',
    'South China Morning Post', 'Nikkei Asia', 'CoinDesk',
}


class NewsProvider(BaseProvider):
    """Aggregates financial news from multiple sources."""

    def __init__(self, config: dict):
        brief_cfg = config.get('daily_brief', {})
        news_cfg = brief_cfg.get('news', {})
        cache_ttl = brief_cfg.get('cache', {}).get('news_ttl', 1800)
        cache_dir = config.get('cache', {}).get('dir', 'cache')

        super().__init__(config, cache_dir=f"{cache_dir}/brief/news", cache_ttl=cache_ttl)

        self.max_articles = news_cfg.get('max_articles', 30)
        self.top_n = news_cfg.get('top_n', 5)
        self.finnhub_key = news_cfg.get('finnhub_api_key', '')

    def fetch_news(self, keywords: List[str] = None) -> dict:
        """
        Fetch and aggregate news from all sources.
        Returns: {status, articles: [...], top5: [...], timestamp}
        """
        all_articles = []

        # 1. Google News RSS - topic feeds
        for topic_name, topic_id in TOPICS.items():
            try:
                articles = self._fetch_google_topic(topic_id, topic_name)
                all_articles.extend(articles)
            except Exception as e:
                logger.warning(f"Google News topic {topic_name} failed: {e}")

        # 2. Google News RSS - keyword searches
        search_terms = keywords or FINANCE_KEYWORDS
        for term in search_terms[:6]:  # Limit to avoid rate limiting
            try:
                articles = self._fetch_google_search(term)
                all_articles.extend(articles)
            except Exception as e:
                logger.warning(f"Google News search '{term}' failed: {e}")

        # 3. Finnhub (if key available)
        if self.finnhub_key:
            try:
                articles = self._fetch_finnhub()
                all_articles.extend(articles)
            except Exception as e:
                logger.warning(f"Finnhub news failed: {e}")

        # Deduplicate
        unique = self._deduplicate(all_articles)

        # Score and rank
        scored = self._score_articles(unique)
        scored.sort(key=lambda a: a.get('relevance_score', 0), reverse=True)

        # Top N
        top_n = scored[:self.top_n]

        return {
            'status': 'ok' if scored else 'empty',
            'articles': scored[:self.max_articles],
            'top5': top_n,
            'total_fetched': len(all_articles),
            'total_unique': len(unique),
            'timestamp': datetime.now().isoformat(),
        }

    def _fetch_google_topic(self, topic_id: str, topic_name: str) -> List[dict]:
        """Fetch Google News RSS by topic."""
        url = f"{GOOGLE_NEWS_RSS}/topics/{topic_id}"
        return self._parse_rss(url, source_tag=f"google:{topic_name}")

    def _fetch_google_search(self, query: str) -> List[dict]:
        """Fetch Google News RSS by search query."""
        url = f"{GOOGLE_NEWS_SEARCH}?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        return self._parse_rss(url, source_tag=f"google:search:{query}")

    def _parse_rss(self, url: str, source_tag: str = "") -> List[dict]:
        """Parse RSS XML feed into article dicts."""
        resp = http_get(url, timeout=15)
        root = ET.fromstring(resp.text)

        articles = []
        for item in root.findall('.//item'):
            title = (item.findtext('title') or '').strip()
            link = (item.findtext('link') or '').strip()
            pub_date = (item.findtext('pubDate') or '').strip()
            description = (item.findtext('description') or '').strip()

            # Extract source from title (Google News format: "Title - Source")
            source_name = ''
            if ' - ' in title:
                parts = title.rsplit(' - ', 1)
                if len(parts) == 2:
                    title = parts[0].strip()
                    source_name = parts[1].strip()

            # Clean HTML from description
            description = re.sub(r'<[^>]+>', '', description).strip()
            if len(description) > 300:
                description = description[:297] + '...'

            # Parse date
            parsed_date = self._parse_date(pub_date)

            articles.append({
                'title': title,
                'url': link,
                'source': source_name,
                'published': parsed_date,
                'published_raw': pub_date,
                'summary': description,
                'fetch_source': source_tag,
            })

        return articles[:15]  # Limit per feed

    def _fetch_finnhub(self) -> List[dict]:
        """Fetch from Finnhub news API."""
        url = "https://finnhub.io/api/v1/news"
        params = {
            'category': 'general',
            'token': self.finnhub_key,
        }
        resp = http_get(url, params=params, timeout=15)
        data = resp.json()

        articles = []
        for item in data[:20]:
            articles.append({
                'title': item.get('headline', ''),
                'url': item.get('url', ''),
                'source': item.get('source', ''),
                'published': datetime.fromtimestamp(item.get('datetime', 0)).isoformat()
                             if item.get('datetime') else None,
                'summary': item.get('summary', '')[:300],
                'fetch_source': 'finnhub',
            })
        return articles

    def _deduplicate(self, articles: List[dict]) -> List[dict]:
        """Remove duplicate articles based on title similarity."""
        unique = []
        seen_titles = []

        for article in articles:
            title = article.get('title', '')
            if not title:
                continue

            # Check similarity with existing titles
            is_dup = False
            for seen in seen_titles:
                ratio = SequenceMatcher(None, title.lower(), seen.lower()).ratio()
                if ratio > 0.7:
                    is_dup = True
                    break

            if not is_dup:
                unique.append(article)
                seen_titles.append(title)

        return unique

    def _score_articles(self, articles: List[dict]) -> List[dict]:
        """Score articles by relevance, timeliness, and source authority."""
        now = datetime.now()

        for article in articles:
            score = 50.0  # Base score

            # Timeliness (50% weight, max 50 points)
            pub = article.get('published')
            if pub:
                try:
                    pub_dt = datetime.fromisoformat(pub.replace('Z', '+00:00'))
                    if pub_dt.tzinfo:
                        pub_dt = pub_dt.replace(tzinfo=None)
                    hours_old = (now - pub_dt).total_seconds() / 3600
                    if hours_old < 6:
                        score += 50
                    elif hours_old < 12:
                        score += 40
                    elif hours_old < 24:
                        score += 25
                    elif hours_old < 48:
                        score += 10
                except (ValueError, TypeError):
                    pass

            # Source authority (30% weight, max 30 points)
            source = article.get('source', '')
            if source in HIGH_AUTHORITY_SOURCES:
                score += 30
            elif any(s.lower() in source.lower() for s in HIGH_AUTHORITY_SOURCES):
                score += 20

            # Keyword relevance (20% weight, max 20 points)
            title = article.get('title', '').lower()
            market_keywords = ['stock', 'market', 'fed', 'rate', 'bitcoin', 'crypto',
                             'earnings', 'ipo', 'merger', 'tariff', 'inflation',
                             'recession', 'rally', 'crash', 'surge', 'plunge',
                             'investment', 'fund', 'tech', 'ai', 'nvidia', 'tesla']
            hits = sum(1 for kw in market_keywords if kw in title)
            score += min(hits * 5, 20)

            article['relevance_score'] = round(score, 1)

        return articles

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO format."""
        if not date_str:
            return None
        # RFC 2822 (common in RSS)
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.isoformat()
            except ValueError:
                continue
        return date_str

    # BaseProvider interface
    def _fetch_impl(self, key: str, **kwargs) -> dict:
        return self.fetch_news(**kwargs)

    def _fallback_impl(self, key: str, **kwargs) -> dict:
        return {
            'status': 'fallback',
            'articles': [],
            'top5': [],
            'timestamp': datetime.now().isoformat(),
        }


def search_news_for_stock(stock_name: str, stock_code: str) -> List[dict]:
    """
    Search news for a specific stock (used by movers_service for reason attribution).
    Returns list of relevant news articles.
    """
    results = []
    queries = [f'"{stock_name}" stock', stock_code]

    for q in queries:
        try:
            url = f"{GOOGLE_NEWS_SEARCH}?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
            resp = http_get(url, timeout=10)
            root = ET.fromstring(resp.text)

            for item in root.findall('.//item')[:5]:
                title = (item.findtext('title') or '').strip()
                link = (item.findtext('link') or '').strip()
                source = ''
                if ' - ' in title:
                    parts = title.rsplit(' - ', 1)
                    title = parts[0].strip()
                    source = parts[1].strip() if len(parts) > 1 else ''
                results.append({
                    'title': title,
                    'url': link,
                    'source': source,
                })

            if results:
                break
        except Exception as e:
            logger.debug(f"News search for {stock_name} failed: {e}")
            continue

    return results[:3]
