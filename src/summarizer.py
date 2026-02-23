"""
summarizer.py - Generate structured summary for LLM consumption
Outputs summary_for_llm.json with all key information condensed.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class Summarizer:
    """Generate structured JSON summary for downstream LLM analysis."""

    def __init__(self, config: dict):
        self.output_dir = Path(config.get("output", {}).get("base_dir", "output"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, panel: pd.DataFrame, signal_panel: pd.DataFrame,
                 judgment: dict, data_quality: dict) -> dict:
        """Build and save summary_for_llm.json."""
        summary = {
            "meta": {
                "generated_at": datetime.now().isoformat(),
                "report_date": judgment.get("date", str(datetime.now().date())),
                "data_range": {
                    "start": str(panel.index[0].date()) if len(panel) > 0 else None,
                    "end": str(panel.index[-1].date()) if len(panel) > 0 else None,
                    "trading_days": len(panel),
                },
            },
            "judgment": {
                "regime": judgment.get("regime"),
                "regime_cn": judgment.get("regime_cn"),
                "confidence": judgment.get("confidence"),
                "explanation": judgment.get("explanation"),
                "net_liquidity_weakening": judgment.get("net_liquidity_weakening"),
                "stress_count": judgment.get("stress_count"),
                "stress_dimensions": judgment.get("stress_dimensions"),
                "risk_asset_confirming": judgment.get("risk_asset_confirming"),
            },
            "latest_readings": self._extract_latest(panel, signal_panel),
            "changes_summary": self._extract_changes(signal_panel),
            "data_quality": self._simplify_quality(data_quality),
            "dimension_details": judgment.get("dimension_details", {}),
        }

        # Save to file
        output_path = self.output_dir / "summary_for_llm.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Summary saved: {output_path}")
        return summary

    def _extract_latest(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        """Extract latest readings for key indicators."""
        key_cols = [
            "net_liquidity", "sofr", "hy_oas", "vix", "move_proxy",
            "usdjpy", "carry_spread_bps", "curve_slope_bps",
            "spx", "btc", "dxy", "us2y", "us10y",
        ]
        readings = {}
        for col in key_cols:
            if col in panel.columns and panel[col].notna().any():
                val = float(panel[col].dropna().iloc[-1])
                zscore_col = f"{col}_zscore"
                pctl_col = f"{col}_pctl"
                signal_col = f"{col}_signal"

                entry = {"value": round(val, 4)}
                if zscore_col in signal.columns and signal[zscore_col].notna().any():
                    entry["zscore"] = round(float(signal[zscore_col].dropna().iloc[-1]), 2)
                if pctl_col in signal.columns and signal[pctl_col].notna().any():
                    entry["percentile"] = round(float(signal[pctl_col].dropna().iloc[-1]), 3)
                if signal_col in signal.columns and signal[signal_col].notna().any():
                    entry["signal"] = str(signal[signal_col].dropna().iloc[-1])

                readings[col] = entry

        return readings

    def _extract_changes(self, signal: pd.DataFrame) -> dict:
        """Extract 1d/5d/20d changes for key indicators."""
        key_indicators = [
            "net_liquidity", "sofr", "hy_oas", "vix", "move_proxy",
            "usdjpy", "carry_spread_bps", "spx", "btc",
        ]
        changes = {}
        for ind in key_indicators:
            entry = {}
            for window in [1, 5, 20]:
                chg_col = f"{ind}_chg_{window}d"
                pct_col = f"{ind}_pct_{window}d"

                if chg_col in signal.columns and signal[chg_col].notna().any():
                    entry[f"chg_{window}d"] = round(float(signal[chg_col].dropna().iloc[-1]), 4)
                if pct_col in signal.columns and signal[pct_col].notna().any():
                    entry[f"pct_{window}d"] = round(float(signal[pct_col].dropna().iloc[-1]), 4)

            if entry:
                changes[ind] = entry

        return changes

    def _simplify_quality(self, data_quality: dict) -> dict:
        """Simplify data quality report for JSON output."""
        simplified = {}
        for key, info in data_quality.items():
            if isinstance(info, dict):
                simplified[key] = {
                    "status": info.get("status", "unknown"),
                    "coverage": info.get("coverage"),
                    "stale_days": info.get("stale_days"),
                    "last_valid": info.get("last_valid"),
                }
        return simplified
