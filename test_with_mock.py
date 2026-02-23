#!/usr/bin/env python3
"""
test_with_mock.py - Test full pipeline with realistic mock data
Validates: cleaner -> indicators -> signals -> judge -> charts -> summary -> report
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import yaml
except ImportError:
    yaml = None

from run_daily import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("test_mock")


def generate_mock_data() -> dict:
    """Generate ~1 year of realistic mock macro data."""
    np.random.seed(42)
    dates = pd.bdate_range(start="2025-02-01", end="2026-02-20")
    n = len(dates)

    def brownian(start, vol, n):
        return start + np.cumsum(np.random.normal(0, vol, n))

    def mean_reverting(center, vol, n, speed=0.05):
        x = np.zeros(n)
        x[0] = center
        for i in range(1, n):
            x[i] = x[i-1] + speed * (center - x[i-1]) + np.random.normal(0, vol)
        return x

    raw = {}

    # Fed Total Assets (billions, ~7000-7200 range, slow drift)
    raw["fed_total_assets"] = pd.DataFrame(
        {"value": brownian(7100, 5, n)},
        index=dates
    )

    # TGA (billions, 500-900 range, more volatile)
    raw["tga_balance"] = pd.DataFrame(
        {"value": np.clip(mean_reverting(700, 20, n), 300, 1200)},
        index=dates
    )

    # ON RRP (billions, declining trend from 500 to ~200)
    rrp_trend = np.linspace(500, 150, n) + np.random.normal(0, 15, n)
    raw["on_rrp"] = pd.DataFrame(
        {"value": np.clip(rrp_trend, 50, 700)},
        index=dates
    )

    # SOFR (percent, ~4.3-4.5 range)
    raw["sofr"] = pd.DataFrame(
        {"value": mean_reverting(4.35, 0.02, n, speed=0.1)},
        index=dates
    )

    # HY OAS (percent, ~3-5 range)
    raw["hy_oas"] = pd.DataFrame(
        {"value": np.clip(mean_reverting(3.5, 0.1, n, speed=0.03), 2.5, 7.0)},
        index=dates
    )

    # US 2Y Yield
    raw["us2y"] = pd.DataFrame(
        {"value": mean_reverting(4.2, 0.05, n, speed=0.02)},
        index=dates
    )

    # US 10Y Yield
    raw["us10y"] = pd.DataFrame(
        {"value": mean_reverting(4.5, 0.04, n, speed=0.02)},
        index=dates
    )

    # VIX
    raw["vix"] = pd.DataFrame(
        {"value": np.clip(mean_reverting(18, 2, n, speed=0.08), 10, 50)},
        index=dates
    )

    # USD/JPY
    raw["usdjpy"] = pd.DataFrame(
        {"value": brownian(150, 0.8, n)},
        index=dates
    )

    # JP 2Y (monthly-ish, sparse)
    jp_monthly = pd.bdate_range(start="2025-02-01", end="2026-02-20", freq="MS")
    raw["jp2y"] = pd.DataFrame(
        {"value": mean_reverting(0.35, 0.03, len(jp_monthly), speed=0.1)},
        index=jp_monthly
    )

    # S&P 500
    raw["spx"] = pd.DataFrame(
        {"value": brownian(5800, 30, n)},
        index=dates
    )

    # DXY
    raw["dxy"] = pd.DataFrame(
        {"value": mean_reverting(104, 0.5, n, speed=0.03)},
        index=dates
    )

    # BTC
    raw["btc"] = pd.DataFrame(
        {"value": np.clip(brownian(95000, 2000, n), 50000, 150000)},
        index=dates
    )

    # Add index names
    for key in raw:
        raw[key].index.name = "date"

    return raw


def main():
    logger.info("=" * 60)
    logger.info("MOCK DATA PIPELINE TEST")
    logger.info("=" * 60)

    # Load config
    config_path = PROJECT_ROOT / "config.yaml"
    config = load_config(config_path)
    config["cache"]["dir"] = str(PROJECT_ROOT / "cache")
    config["output"]["base_dir"] = str(PROJECT_ROOT / "output")

    # Generate mock data
    logger.info("Generating mock data...")
    raw_data = generate_mock_data()
    logger.info(f"Generated {len(raw_data)} indicators")

    # Phase 2: Clean
    logger.info("Cleaning data...")
    from src.cleaner import DataCleaner
    cleaner = DataCleaner(config)
    panel = cleaner.build_daily_panel(raw_data)
    quality = cleaner.get_quality_report()
    logger.info(f"Panel: {panel.shape}")

    # Phase 3: Indicators
    logger.info("Computing indicators...")
    from src.indicators import IndicatorEngine
    ind_engine = IndicatorEngine(config)
    panel = ind_engine.compute(panel)
    logger.info(f"Panel with indicators: {panel.shape}")
    logger.info(f"Columns: {list(panel.columns)}")

    # Phase 4: Signals
    logger.info("Computing signals...")
    from src.signals import SignalEngine
    sig_engine = SignalEngine(config)
    signal_panel = sig_engine.compute(panel)
    logger.info(f"Signal panel: {signal_panel.shape}")

    # Phase 5: Judgment
    logger.info("Running judgment...")
    from src.judge import JudgmentEngine
    judge = JudgmentEngine(config)
    judgment = judge.evaluate(panel, signal_panel, quality)
    logger.info(f"REGIME: {judgment['regime_cn']} ({judgment['regime']})")
    logger.info(f"Confidence: {judgment['confidence']}")
    logger.info(f"Explanation: {judgment['explanation']}")

    # Phase 6: Save outputs
    from pathlib import Path
    output_dir = Path(config["output"]["base_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    panel.to_csv(output_dir / "daily_panel.csv")
    signal_panel.to_csv(output_dir / "signal_panel.csv")
    logger.info("CSVs saved")

    # Phase 7: Summary
    logger.info("Generating summary...")
    from src.summarizer import Summarizer
    summarizer = Summarizer(config)
    summary = summarizer.generate(panel, signal_panel, judgment, quality)

    # Phase 8: Charts
    logger.info("Generating charts...")
    from src.charter import ChartEngine
    charter = ChartEngine(config)
    chart_files = charter.generate_all(panel)
    logger.info(f"Generated {len(chart_files)} charts")

    # Phase 9: Report
    logger.info("Generating report...")
    from src.reporter import ReportGenerator
    reporter = ReportGenerator(config)
    report = reporter.generate(summary)

    # Summary output
    print(f"\n{'='*50}")
    print(f"  MOCK TEST COMPLETE")
    print(f"  Regime: {judgment['regime_cn']} ({judgment['regime']})")
    print(f"  Confidence: {judgment['confidence']}")
    print(f"  Stress count: {judgment['stress_count']}")
    print(f"  Stress dims: {judgment['stress_dimensions']}")
    print(f"{'='*50}")
    print(f"  Panel: {panel.shape[0]} days x {panel.shape[1]} cols")
    print(f"  Signals: {signal_panel.shape[1]} columns")
    print(f"  Charts: {len(chart_files)} files")
    print(f"  Output: {output_dir}/")
    print(f"{'='*50}\n")

    # Print last row of key indicators
    print("Latest readings:")
    key_cols = ["net_liquidity", "sofr", "hy_oas", "move_proxy", "usdjpy",
                "carry_spread_bps", "vix", "spx", "curve_slope_bps"]
    for col in key_cols:
        if col in panel.columns:
            val = panel[col].dropna().iloc[-1]
            print(f"  {col:>25s}: {val:.2f}")


if __name__ == "__main__":
    main()
