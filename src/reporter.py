"""
reporter.py - Daily report text generator (optional)
Generates a markdown daily report from the judgment and summary.
"""

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generate a markdown daily liquidity report."""

    def __init__(self, config: dict):
        self.output_dir = Path(config.get("output", {}).get("base_dir", "output"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, summary: dict) -> str:
        """Generate markdown report text and save to file."""
        j = summary.get("judgment", {})
        readings = summary.get("latest_readings", {})
        changes = summary.get("changes_summary", {})
        quality = summary.get("data_quality", {})
        details = summary.get("dimension_details", {})
        meta = summary.get("meta", {})

        report_date = meta.get("report_date", str(datetime.now().date()))

        lines = []
        lines.append(f"# Macro Liquidity Daily Report")
        lines.append(f"**Date: {report_date}**\n")

        # Regime banner
        regime_emoji = {
            "TIGHTENING": "RED",
            "LOCAL_DISTURBANCE": "YELLOW",
            "STABLE": "GREEN",
            "UNKNOWN": "GRAY",
        }
        regime = j.get("regime", "UNKNOWN")
        banner = regime_emoji.get(regime, "GRAY")
        lines.append(f"## Regime: [{banner}] {j.get('regime_cn', '?')} ({regime})")
        lines.append(f"**Confidence: {j.get('confidence', '?')}**\n")
        lines.append(f"> {j.get('explanation', '')}\n")

        # Key metrics table
        lines.append("## Key Metrics\n")
        lines.append("| Indicator | Latest | 5d Change | Z-Score | Signal |")
        lines.append("|-----------|--------|-----------|---------|--------|")

        metric_labels = {
            "net_liquidity": "Net Liquidity (B)",
            "sofr": "SOFR (%)",
            "hy_oas": "HY OAS (%)",
            "move_proxy": "MOVE Proxy",
            "usdjpy": "USD/JPY",
            "carry_spread_bps": "Carry Spread (bps)",
            "vix": "VIX",
            "spx": "S&P 500",
            "dxy": "DXY",
            "btc": "BTC (USD)",
            "curve_slope_bps": "10Y-2Y Slope (bps)",
        }

        for key, label in metric_labels.items():
            r = readings.get(key, {})
            c = changes.get(key, {})
            val = r.get("value", "N/A")
            chg_5d = c.get("chg_5d", c.get("pct_5d", "N/A"))
            if isinstance(chg_5d, float) and key in ("spx", "btc", "usdjpy", "dxy"):
                chg_5d_str = f"{c.get('pct_5d', 0)*100:.1f}%" if "pct_5d" in c else f"{chg_5d:.2f}"
            elif isinstance(chg_5d, float):
                chg_5d_str = f"{chg_5d:.2f}"
            else:
                chg_5d_str = str(chg_5d)

            zscore = r.get("zscore", "N/A")
            signal = r.get("signal", "N/A")

            if isinstance(val, float):
                val_str = f"{val:,.2f}" if abs(val) > 100 else f"{val:.4f}"
            else:
                val_str = str(val)

            lines.append(f"| {label} | {val_str} | {chg_5d_str} | {zscore} | {signal} |")

        # Dimension details
        lines.append("\n## Dimension Analysis\n")
        for dim_name, dim_info in details.items():
            if isinstance(dim_info, dict):
                status = "STRESS" if dim_info.get("stress") or dim_info.get("weakening") or dim_info.get("confirming_weakness") else "OK"
                lines.append(f"- **{dim_name}** [{status}]: {dim_info.get('detail', 'N/A')}")

        # Data quality notes
        degraded = [k for k, v in quality.items() if isinstance(v, dict) and v.get("status") != "ok"]
        if degraded:
            lines.append("\n## Data Quality Warnings\n")
            for k in degraded:
                q = quality[k]
                lines.append(f"- {k}: {q.get('status', '?')} (coverage: {q.get('coverage', '?')}, stale: {q.get('stale_days', '?')}d)")

        # Footer
        lines.append(f"\n---\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                      f"MOVE is a proxy indicator (VIX-based) | JP 2Y may be interpolated*")

        report_text = "\n".join(lines)

        # Save
        output_path = self.output_dir / "daily_report.md"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info(f"Report saved: {output_path}")
        return report_text
