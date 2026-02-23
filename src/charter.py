"""
charter.py - Chart generation module
Generates 1-year trend charts for each key indicator.
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logger = logging.getLogger(__name__)


class ChartEngine:
    """Generate trend charts from panel data."""

    def __init__(self, config: dict):
        self.output_cfg = config.get("output", {})
        self.chart_dir = Path(self.output_cfg.get("base_dir", "output")) / "charts"
        self.chart_dir.mkdir(parents=True, exist_ok=True)
        self.dpi = self.output_cfg.get("chart_dpi", 150)
        self.lookback = self.output_cfg.get("chart_lookback_days", 252)

        try:
            plt.style.use(self.output_cfg.get("chart_style", "seaborn-v0_8-darkgrid"))
        except Exception:
            plt.style.use("ggplot")

    # Charts to generate: (column, title, ylabel, invert_y, add_annotations)
    CHART_SPECS = [
        {
            "col": "net_liquidity",
            "title": "Net Liquidity (Fed Assets - TGA - ON RRP)",
            "ylabel": "Billions USD",
            "color": "#2196F3",
        },
        {
            "col": "sofr",
            "title": "SOFR (Secured Overnight Financing Rate)",
            "ylabel": "Rate (%)",
            "color": "#FF5722",
        },
        {
            "col": "move_proxy",
            "title": "Rate Volatility Proxy (VIX-based MOVE approximation)",
            "ylabel": "Index",
            "color": "#9C27B0",
        },
        {
            "col": "usdjpy",
            "title": "USD/JPY",
            "ylabel": "JPY per USD",
            "color": "#4CAF50",
        },
        {
            "col": "carry_spread_bps",
            "title": "Carry Spread (US 2Y - JP 2Y)",
            "ylabel": "Basis Points",
            "color": "#FF9800",
        },
        {
            "col": "hy_oas",
            "title": "HY OAS (ICE BofA US High Yield Spread)",
            "ylabel": "OAS (%)",
            "color": "#F44336",
        },
        {
            "col": "vix",
            "title": "VIX",
            "ylabel": "Index",
            "color": "#E91E63",
        },
        {
            "col": "spx",
            "title": "S&P 500",
            "ylabel": "Index",
            "color": "#3F51B5",
        },
        {
            "col": "curve_slope_bps",
            "title": "Yield Curve Slope (10Y - 2Y)",
            "ylabel": "Basis Points",
            "color": "#009688",
        },
        {
            "col": "dxy",
            "title": "US Dollar Index (DXY)",
            "ylabel": "Index",
            "color": "#795548",
        },
        {
            "col": "btc",
            "title": "Bitcoin (BTC/USD)",
            "ylabel": "USD",
            "color": "#FFC107",
        },
    ]

    def generate_all(self, panel: pd.DataFrame) -> list:
        """Generate all charts. Returns list of generated file paths."""
        generated = []

        # Trim to lookback period
        cutoff = panel.index[-1] - pd.Timedelta(days=self.lookback)
        df = panel[panel.index >= cutoff].copy()

        for spec in self.CHART_SPECS:
            col = spec["col"]
            if col not in df.columns:
                logger.warning(f"Chart skipped: {col} not in panel")
                continue

            series = df[col].dropna()
            if len(series) < 5:
                logger.warning(f"Chart skipped: {col} has < 5 data points")
                continue

            try:
                path = self._plot_single(series, spec)
                generated.append(str(path))
                logger.info(f"Chart saved: {path}")
            except Exception as e:
                logger.error(f"Chart error for {col}: {e}")

        # Composite: Net Liquidity + SPX overlay
        if "net_liquidity" in df.columns and "spx" in df.columns:
            try:
                path = self._plot_composite(df)
                generated.append(str(path))
            except Exception as e:
                logger.error(f"Composite chart error: {e}")

        return generated

    def _plot_single(self, series: pd.Series, spec: dict) -> Path:
        """Plot a single indicator trend chart with auto-scaled Y-axis."""
        fig, ax = plt.subplots(figsize=(12, 5))

        color = spec.get("color", "#2196F3")

        ax.plot(series.index, series.values, color=color,
                linewidth=1.5, alpha=0.9)

        # Auto-scale Y-axis with padding (NOT from zero)
        ymin, ymax = series.min(), series.max()
        yrange = ymax - ymin
        pad = yrange * 0.08 if yrange > 0 else abs(ymin) * 0.05
        ax.set_ylim(ymin - pad, ymax + pad)

        # Fill between the bottom of the visible area and the line
        ax.fill_between(series.index, ymin - pad, series.values,
                        alpha=0.08, color=color)

        # Latest value annotation
        latest_val = series.iloc[-1]
        latest_date = series.index[-1]
        if abs(latest_val) >= 1000:
            val_label = f"{latest_val:,.0f}"
        elif abs(latest_val) >= 10:
            val_label = f"{latest_val:.2f}"
        else:
            val_label = f"{latest_val:.4f}"

        ax.annotate(
            val_label,
            xy=(latest_date, latest_val),
            xytext=(10, 10),
            textcoords="offset points",
            fontsize=10,
            fontweight="bold",
            color=color,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8),
        )

        # 20-day moving average
        if len(series) >= 20:
            ma20 = series.rolling(20).mean()
            ax.plot(ma20.index, ma20.values, color="gray", linewidth=1, linestyle="--",
                    alpha=0.6, label="20d MA")
            ax.legend(loc="upper left", fontsize=8)

        ax.set_title(spec["title"], fontsize=13, fontweight="bold", pad=10)
        ax.set_ylabel(spec.get("ylabel", ""), fontsize=10)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        filename = self.chart_dir / f"{spec['col']}.png"
        fig.savefig(filename, dpi=self.dpi, bbox_inches="tight")
        plt.close(fig)
        return filename

    def _plot_composite(self, df: pd.DataFrame) -> Path:
        """Composite chart: Net Liquidity (left axis) vs SPX (right axis)."""
        fig, ax1 = plt.subplots(figsize=(12, 5))

        nl = df["net_liquidity"].dropna()
        spx = df["spx"].dropna()

        # Left axis: Net Liquidity
        ax1.plot(nl.index, nl.values, color="#2196F3", linewidth=1.5, label="Net Liquidity")
        ax1.set_ylabel("Net Liquidity (Billions USD)", color="#2196F3", fontsize=10)
        ax1.tick_params(axis="y", labelcolor="#2196F3")

        # Right axis: SPX
        ax2 = ax1.twinx()
        ax2.plot(spx.index, spx.values, color="#F44336", linewidth=1.2, alpha=0.7, label="S&P 500")
        ax2.set_ylabel("S&P 500", color="#F44336", fontsize=10)
        ax2.tick_params(axis="y", labelcolor="#F44336")

        ax1.set_title("Net Liquidity vs S&P 500 (Risk Asset Confirmation)", fontsize=13,
                       fontweight="bold", pad=10)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)

        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8)

        fig.tight_layout()
        filename = self.chart_dir / "composite_netliq_spx.png"
        fig.savefig(filename, dpi=self.dpi, bbox_inches="tight")
        plt.close(fig)
        logger.info(f"Composite chart saved: {filename}")
        return filename
