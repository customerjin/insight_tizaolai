"""
cleaner.py - Data cleaning and alignment to daily panel
Handles: frequency conversion, forward-fill, interpolation, unit normalization.
"""

import logging
from typing import Dict
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DataCleaner:
    """Clean and align raw fetched data into a unified daily panel."""

    def __init__(self, config: dict):
        self.config = config
        self.data_quality = {}  # Track quality per indicator

    def build_daily_panel(self, raw_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Merge all indicators into a single daily-frequency DataFrame.
        Index: business days. Columns: indicator names.
        """
        if not raw_data:
            raise ValueError("No raw data provided")

        # Determine date range from available data
        all_dates = []
        for df in raw_data.values():
            if len(df) > 0:
                all_dates.extend([df.index.min(), df.index.max()])

        if not all_dates:
            raise ValueError("All data is empty")

        start = min(all_dates)
        end = max(all_dates)

        # Create business day index
        bdays = pd.bdate_range(start=start, end=end)
        panel = pd.DataFrame(index=bdays)
        panel.index.name = "date"

        # Process each indicator
        indicators_cfg = self.config.get("indicators", {})
        yahoo_cfg = self.config.get("yahoo_sources", {})

        for key, df in raw_data.items():
            if df is None or len(df) == 0:
                logger.warning(f"Skipping {key}: empty data")
                self.data_quality[key] = {"status": "missing", "coverage": 0.0}
                continue

            # Extract value column
            if "value" in df.columns:
                series = df["value"].copy()
            elif len(df.columns) == 1:
                series = df.iloc[:, 0].copy()
            else:
                series = df["value"].copy() if "value" in df.columns else df.iloc[:, 0].copy()

            series = pd.to_numeric(series, errors="coerce")

            # Determine frequency & fill method
            freq_info = self._get_frequency(key, indicators_cfg, yahoo_cfg)

            if freq_info == "monthly":
                # Resample monthly -> daily with linear interpolation
                series = series.reindex(bdays)
                series = series.interpolate(method="time")
                fill_label = "interpolated"
            elif freq_info == "weekly":
                # Forward-fill weekly data
                series = series.reindex(bdays, method="ffill")
                fill_label = "ffill_weekly"
            else:
                # Daily: reindex and ffill gaps (max 3 days)
                series = series.reindex(bdays)
                series = series.ffill(limit=3)
                fill_label = "ffill_daily"

            # Unit normalization
            series = self._normalize_units(key, series, indicators_cfg)

            panel[key] = series

            # Quality tracking
            total = len(panel)
            non_null = series.notna().sum()
            coverage = non_null / total if total > 0 else 0
            stale_days = self._count_stale_days(series)

            self.data_quality[key] = {
                "status": "ok" if coverage > 0.8 else "degraded",
                "coverage": round(coverage, 3),
                "fill_method": fill_label,
                "stale_days": stale_days,
                "last_valid": str(series.last_valid_index().date()) if series.last_valid_index() is not None else None,
            }

        logger.info(f"Daily panel built: {panel.shape[0]} days x {panel.shape[1]} indicators")
        return panel

    def _get_frequency(self, key: str, indicators_cfg: dict, yahoo_cfg: dict) -> str:
        """Determine the native frequency of an indicator."""
        if key in indicators_cfg:
            return indicators_cfg[key].get("frequency", "daily")
        if key == "jp2y":
            return self.config.get("jp2y", {}).get("frequency", "monthly")
        # Yahoo sources are daily
        return "daily"

    def _normalize_units(self, key: str, series: pd.Series, indicators_cfg: dict) -> pd.Series:
        """
        Normalize units for consistent calculation.
        - WALCL (millions) -> billions
        - WTREGEN (millions) -> billions
        - ON RRP already in billions
        """
        if key in indicators_cfg:
            unit = indicators_cfg[key].get("unit", "")
            if key in ("fed_total_assets", "tga_balance") and unit == "millions":
                series = series / 1000.0  # millions -> billions
                logger.info(f"Converted {key} from millions to billions")
        return series

    def _count_stale_days(self, series: pd.Series) -> int:
        """Count how many trailing days are NaN (stale data)."""
        if series.empty or series.isna().all():
            return len(series)
        last_valid = series.last_valid_index()
        if last_valid is None:
            return len(series)
        tail = series.loc[last_valid:]
        return len(tail) - 1  # Days after last valid observation

    def get_quality_report(self) -> dict:
        """Return data quality summary."""
        return self.data_quality
