"""
fetcher.py - Data fetching module
Supports: FRED API (primary), Yahoo Finance (FX/equities), Ivo Welch gateway (backup)
All via raw HTTP requests (no pip dependencies beyond requests/pandas).
"""

import os
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ============================================================
# Constants
# ============================================================
FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_CSV_DIRECT = "https://fred.stlouisfed.org/graph/fredgraph.csv"  # No-key fallback
YAHOO_CHART_BASE = "https://query2.finance.yahoo.com/v8/finance/chart"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Retry config
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


# ============================================================
# Helper: HTTP GET with retries
# ============================================================
def _http_get(url: str, params: Optional[dict] = None, timeout: int = 30) -> requests.Response:
    """HTTP GET with retries and exponential backoff."""
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:  # Rate limit
                wait = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Rate limited, waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt+1})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error: {e} (attempt {attempt+1})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    raise ConnectionError(f"Failed to fetch {url} after {MAX_RETRIES} attempts")


# ============================================================
# Cache Layer
# ============================================================
class DataCache:
    """Simple file-based cache for raw data."""

    def __init__(self, cache_dir: str, max_age_hours: int = 12):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_age = timedelta(hours=max_age_hours)

    def _key_path(self, key: str) -> Path:
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{safe_key}.csv"

    def get(self, key: str) -> Optional[pd.DataFrame]:
        path = self._key_path(key)
        if not path.exists():
            return None
        mod_time = datetime.fromtimestamp(path.stat().st_mtime)
        if datetime.now() - mod_time > self.max_age:
            logger.info(f"Cache expired for {key}")
            return None
        try:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            logger.info(f"Cache hit for {key} ({len(df)} rows)")
            return df
        except Exception as e:
            logger.warning(f"Cache read error for {key}: {e}")
            return None

    def put(self, key: str, df: pd.DataFrame):
        path = self._key_path(key)
        df.to_csv(path)
        logger.info(f"Cached {key} ({len(df)} rows)")


# ============================================================
# FRED Fetcher
# ============================================================
class FREDFetcher:
    """Fetch data from FRED. Supports API key and no-key fallback."""

    def __init__(self, api_key: str = ""):
        self.api_key = api_key.strip()
        self.has_key = bool(self.api_key)
        if self.has_key:
            logger.info("FRED: Using API key")
        else:
            logger.info("FRED: No API key, using direct CSV fallback (may have limitations)")

    def fetch(self, series_id: str, start_date: str = "2020-01-01",
              end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch a FRED series. Returns DataFrame with DatetimeIndex and 'value' column.
        Tries API key method first, falls back to direct CSV.
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if self.has_key:
            try:
                return self._fetch_with_key(series_id, start_date, end_date)
            except Exception as e:
                logger.warning(f"FRED API failed for {series_id}: {e}, trying fallback")

        return self._fetch_direct_csv(series_id, start_date, end_date)

    def _fetch_with_key(self, series_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "sort_order": "asc",
        }
        resp = _http_get(FRED_API_BASE, params=params)
        data = resp.json()

        if "observations" not in data:
            raise ValueError(f"No observations in FRED response for {series_id}: {data}")

        records = []
        for obs in data["observations"]:
            val = obs["value"]
            if val == ".":  # FRED uses "." for missing
                val = None
            else:
                val = float(val)
            records.append({"date": obs["date"], "value": val})

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df.index.name = "date"
        logger.info(f"FRED API: {series_id} -> {len(df)} observations")
        return df

    def _fetch_direct_csv(self, series_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fallback: Direct CSV download from FRED graph endpoint (no key needed)."""
        params = {
            "id": series_id,
            "cosd": start_date,
            "coed": end_date,
        }
        resp = _http_get(FRED_CSV_DIRECT, params=params)
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))

        # Column names: DATE, <series_id>
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        # Handle missing values (FRED uses "." in CSV too)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.index.name = "date"
        logger.info(f"FRED CSV: {series_id} -> {len(df)} observations")
        return df


# ============================================================
# Yahoo Finance Fetcher
# ============================================================
class YahooFetcher:
    """Fetch daily OHLCV data from Yahoo Finance chart API."""

    def __init__(self):
        self._crumb = None
        self._session = None

    def _get_session(self):
        """Get a session with Yahoo Finance cookie + crumb."""
        if self._session is not None:
            return self._session, self._crumb

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

        # Method 1: Try to get crumb from Yahoo
        try:
            # Visit Yahoo Finance to get cookies
            self._session.get("https://fc.yahoo.com", timeout=10)
            # Get crumb
            crumb_resp = self._session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb",
                timeout=10
            )
            if crumb_resp.status_code == 200 and crumb_resp.text:
                self._crumb = crumb_resp.text
                logger.info(f"Yahoo: got crumb OK")
        except Exception as e:
            logger.warning(f"Yahoo crumb fetch failed: {e}, will try without crumb")
            self._crumb = None

        return self._session, self._crumb

    def fetch(self, ticker: str, period: str = "2y") -> pd.DataFrame:
        """
        Fetch Yahoo Finance data. Returns DataFrame with DatetimeIndex
        and columns: open, high, low, close, volume.
        """
        session, crumb = self._get_session()

        url = f"{YAHOO_CHART_BASE}/{ticker}"
        params = {
            "range": period,
            "interval": "1d",
            "includePrePost": "false",
        }
        if crumb:
            params["crumb"] = crumb

        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                # Fallback: try query1 without crumb
                fallback_url = url.replace("query2.", "query1.")
                logger.warning(f"Yahoo query2 returned {resp.status_code}, trying query1...")
                resp = requests.get(fallback_url, params={
                    "range": period, "interval": "1d", "includePrePost": "false"
                }, headers={"User-Agent": USER_AGENT}, timeout=30)
                resp.raise_for_status()

            data = resp.json()
            return self._parse_chart_response(data, ticker)
        except Exception as e:
            logger.error(f"Yahoo fetch failed for {ticker}: {e}")
            raise

    def _parse_chart_response(self, data: dict, ticker: str) -> pd.DataFrame:
        """Parse Yahoo Finance v8 chart API response."""
        try:
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            quote = result["indicators"]["quote"][0]

            df = pd.DataFrame({
                "date": pd.to_datetime(timestamps, unit="s", utc=True),
                "open": quote.get("open"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "close": quote.get("close"),
                "volume": quote.get("volume"),
            })

            # Convert UTC to US/Eastern date
            df["date"] = df["date"].dt.tz_convert("US/Eastern").dt.date
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            df.index.name = "date"

            # Use close as primary value
            df["value"] = df["close"]
            logger.info(f"Yahoo: {ticker} -> {len(df)} observations")
            return df

        except (KeyError, IndexError, TypeError) as e:
            raise ValueError(f"Failed to parse Yahoo response for {ticker}: {e}")


# ============================================================
# Unified Data Fetcher
# ============================================================
class DataFetcher:
    """
    Orchestrates all data fetching with caching.
    Returns a dict of {indicator_name: pd.DataFrame}.
    """

    def __init__(self, config: dict):
        self.config = config
        self.fred = FREDFetcher(config.get("fred_api_key", ""))
        self.yahoo = YahooFetcher()
        self.cache = DataCache(
            cache_dir=config.get("cache", {}).get("dir", "cache"),
            max_age_hours=config.get("cache", {}).get("max_age_hours", 12),
        )
        self.fetch_log = {}  # Track fetch status per indicator

    def fetch_all(self, start_date: str = "2024-01-01") -> Dict[str, pd.DataFrame]:
        """Fetch all configured indicators. Returns dict of DataFrames."""
        results = {}
        today = datetime.now().strftime("%Y-%m-%d")

        # 1. FRED indicators
        for key, ind_cfg in self.config.get("indicators", {}).items():
            cache_key = f"fred_{ind_cfg['fred_id']}_{start_date}_{today}"
            cached = self.cache.get(cache_key)
            if cached is not None:
                results[key] = cached
                self.fetch_log[key] = {"status": "cached", "source": "fred", "rows": len(cached)}
                continue

            try:
                df = self.fred.fetch(ind_cfg["fred_id"], start_date, today)
                self.cache.put(cache_key, df)
                results[key] = df
                self.fetch_log[key] = {"status": "ok", "source": "fred", "rows": len(df)}
            except Exception as e:
                logger.error(f"Failed to fetch {key} ({ind_cfg['fred_id']}): {e}")
                self.fetch_log[key] = {"status": "error", "source": "fred", "error": str(e)}

        # 2. Yahoo Finance indicators
        for key, yf_cfg in self.config.get("yahoo_sources", {}).items():
            cache_key = f"yahoo_{yf_cfg['ticker']}_{start_date}_{today}"
            cached = self.cache.get(cache_key)
            if cached is not None:
                results[key] = cached
                self.fetch_log[key] = {"status": "cached", "source": "yahoo", "rows": len(cached)}
                continue

            try:
                df = self.yahoo.fetch(yf_cfg["ticker"])
                self.cache.put(cache_key, df)
                results[key] = df
                self.fetch_log[key] = {"status": "ok", "source": "yahoo", "rows": len(df)}
            except Exception as e:
                logger.error(f"Failed to fetch {key} ({yf_cfg['ticker']}): {e}")
                self.fetch_log[key] = {"status": "error", "source": "yahoo", "error": str(e)}

                # Try FRED backup for USDJPY
                if key == "usdjpy":
                    try:
                        logger.info("Trying FRED backup for USDJPY (DEXJPUS)")
                        df = self.fred.fetch("DEXJPUS", start_date, today)
                        self.cache.put(cache_key, df)
                        results[key] = df
                        self.fetch_log[key] = {"status": "ok", "source": "fred_backup", "rows": len(df)}
                    except Exception as e2:
                        logger.error(f"FRED backup also failed for USDJPY: {e2}")

        # 3. Japan 2Y Yield (special handling)
        jp2y_cfg = self.config.get("jp2y", {})
        if jp2y_cfg.get("fred_id"):
            cache_key = f"fred_{jp2y_cfg['fred_id']}_{start_date}_{today}"
            cached = self.cache.get(cache_key)
            if cached is not None:
                results["jp2y"] = cached
                self.fetch_log["jp2y"] = {"status": "cached", "source": "fred", "rows": len(cached)}
            else:
                try:
                    df = self.fred.fetch(jp2y_cfg["fred_id"], start_date, today)
                    self.cache.put(cache_key, df)
                    results["jp2y"] = df
                    self.fetch_log["jp2y"] = {
                        "status": "ok", "source": "fred_monthly",
                        "rows": len(df), "note": "monthly, needs interpolation"
                    }
                except Exception as e:
                    logger.warning(f"JP 2Y fetch failed, using fallback rate: {e}")
                    results["jp2y"] = self._create_fallback_jp2y(
                        start_date, today, jp2y_cfg.get("fallback_rate", 0.5)
                    )
                    self.fetch_log["jp2y"] = {
                        "status": "fallback", "source": "static",
                        "note": f"Using BOJ policy rate {jp2y_cfg.get('fallback_rate', 0.5)}%"
                    }

        return results

    def _create_fallback_jp2y(self, start_date: str, end_date: str, rate: float) -> pd.DataFrame:
        """Create a constant JP 2Y yield series as fallback."""
        dates = pd.bdate_range(start=start_date, end=end_date)
        df = pd.DataFrame({"value": rate}, index=dates)
        df.index.name = "date"
        return df

    def get_fetch_report(self) -> str:
        """Generate a human-readable fetch status report."""
        lines = ["=" * 50, "DATA FETCH REPORT", "=" * 50]
        ok_count = sum(1 for v in self.fetch_log.values() if v["status"] in ("ok", "cached"))
        total = len(self.fetch_log)
        lines.append(f"Success: {ok_count}/{total}")
        lines.append("")

        for key, info in self.fetch_log.items():
            status_icon = {"ok": "[OK]", "cached": "[CACHE]", "error": "[FAIL]", "fallback": "[FALLBACK]"}
            icon = status_icon.get(info["status"], "[?]")
            line = f"  {icon} {key}: {info.get('source', '?')}"
            if "rows" in info:
                line += f" ({info['rows']} rows)"
            if "error" in info:
                line += f" - {info['error'][:60]}"
            if "note" in info:
                line += f" - {info['note']}"
            lines.append(line)

        return "\n".join(lines)


# ============================================================
# Standalone test
# ============================================================
if __name__ == "__main__":
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load config
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    fetcher = DataFetcher(config)
    data = fetcher.fetch_all(start_date="2024-01-01")

    print(fetcher.get_fetch_report())
    print("\n--- Samples ---")
    for key, df in data.items():
        print(f"\n{key}: {df.shape}")
        print(df.tail(3))
