"""
web_export.py - Export pipeline results as a comprehensive JSON for web frontend.

Outputs a single latest.json containing:
- score_data: composite score, individual scores, investment advice
- judgment: regime, explanation, dimensions
- readings: latest values, changes, signals
- charts: base64-encoded PNG images
- quality: data source status
- meta: timestamps, config info
"""

import json
import base64
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class WebExporter:
    """Export all dashboard data as a single JSON file for the web frontend."""

    def __init__(self, config: dict):
        self.output_dir = Path(config.get("output", {}).get("base_dir", "output"))
        self.chart_dir = self.output_dir / "charts"

    def export(self, summary: dict, score_data: dict, web_dir: Path = None) -> str:
        """
        Generate latest.json with all dashboard data.

        Args:
            summary: from Summarizer
            score_data: from MacroScorer
            web_dir: directory to write the JSON (default: output/web/)

        Returns:
            Path to the generated JSON file.
        """
        if web_dir is None:
            web_dir = self.output_dir / "web"
        web_dir.mkdir(parents=True, exist_ok=True)

        # Encode charts as base64
        charts_b64 = {}
        if self.chart_dir.exists():
            for f in sorted(self.chart_dir.iterdir()):
                if f.suffix == ".png":
                    with open(f, "rb") as fh:
                        charts_b64[f.stem] = base64.b64encode(fh.read()).decode()

        # Build the export payload
        payload = {
            "version": "1.0",
            "generated_at": datetime.now().isoformat(),
            "meta": summary.get("meta", {}),

            # Core score
            "score": {
                "composite": score_data["composite_score"],
                "tier": score_data["tier"],
                "tier_cn": score_data["tier_cn"],
                "tier_color": score_data["tier_color"],
                "tier_emoji": score_data["tier_emoji"],
            },

            # Investment advice
            "advice": score_data.get("investment_advice", {}),

            # Asset outlook
            "asset_outlook": score_data.get("risk_asset_outlook", {}),

            # Individual indicator scores
            "indicator_scores": self._clean_scores(score_data.get("individual_scores", {})),

            # Weight table
            "weights": score_data.get("weight_table", {}),

            # Judgment
            "judgment": summary.get("judgment", {}),

            # Latest readings
            "readings": summary.get("latest_readings", {}),

            # Changes
            "changes": summary.get("changes_summary", {}),

            # Dimension details
            "dimensions": summary.get("dimension_details", {}),

            # Data quality
            "quality": summary.get("data_quality", {}),

            # Charts (base64)
            "charts": charts_b64,
        }

        # Write JSON
        output_path = web_dir / "latest.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str)

        size_kb = output_path.stat().st_size // 1024
        logger.info(f"Web export: {output_path} ({size_kb}KB)")
        return str(output_path)

    def _clean_scores(self, scores: dict) -> dict:
        """Ensure all score values are JSON-serializable."""
        clean = {}
        for k, v in scores.items():
            clean[k] = {
                "score": v["score"],
                "signal": v["signal"],
                "signal_cn": v["signal_cn"],
                "signal_color": v["signal_color"],
                "current_value": v["current_value"],
                "percentile": v["percentile"],
                "chg_5d": v["chg_5d"],
                "chg_20d": v["chg_20d"],
                "weight_pct": v["weight_pct"],
            }
        return clean
