"""
storage/cache_manager.py - Unified cache manager for daily brief data.

Extends the existing DataCache pattern to support JSON caching for brief module.
"""

import json
import logging
import hashlib
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BriefCacheManager:
    """Manages all caching for the daily brief module."""

    def __init__(self, config: dict):
        brief_cfg = config.get('daily_brief', {}).get('cache', {})
        base_dir = Path(config.get('cache', {}).get('dir', 'cache'))

        self.cache_dir = base_dir / 'brief'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.ttls = {
            'market': brief_cfg.get('market_ttl', 300),
            'news': brief_cfg.get('news_ttl', 1800),
            'analysis': brief_cfg.get('analysis_ttl', 3600),
            'movers': brief_cfg.get('movers_ttl', 600),
        }

    def get(self, category: str, key: str) -> Optional[Any]:
        """Get cached data by category and key."""
        ttl = self.ttls.get(category, 300)
        path = self._path(category, key)
        if not path.exists():
            return None

        age = time.time() - path.stat().st_mtime
        if age > ttl:
            logger.debug(f"Cache expired: {category}/{key} (age={age:.0f}s > ttl={ttl}s)")
            return None

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.debug(f"Cache hit: {category}/{key}")
            return data
        except Exception as e:
            logger.warning(f"Cache read error {category}/{key}: {e}")
            return None

    def put(self, category: str, key: str, data: Any):
        """Store data in cache."""
        path = self._path(category, key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, default=str)
        except Exception as e:
            logger.warning(f"Cache write error {category}/{key}: {e}")

    def clear(self, category: Optional[str] = None):
        """Clear cache, optionally only for a specific category."""
        import shutil
        if category:
            cat_dir = self.cache_dir / category
            if cat_dir.exists():
                shutil.rmtree(cat_dir)
                logger.info(f"Cleared cache: {category}")
        else:
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Cleared all brief cache")

    def _path(self, category: str, key: str) -> Path:
        safe_key = hashlib.md5(f"{category}:{key}".encode()).hexdigest()
        cat_dir = self.cache_dir / category
        cat_dir.mkdir(parents=True, exist_ok=True)
        return cat_dir / f"{safe_key}.json"
