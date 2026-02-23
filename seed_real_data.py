#!/usr/bin/env python3
"""
seed_real_data.py
Generates realistic data series calibrated to ACTUAL market levels
sourced from FRED, Yahoo Finance, Trading Economics, CNBC, etc.
(as of February 20, 2026)

This is used when the VM cannot directly access external APIs.
On your own machine, run_daily.py fetches live data automatically.
"""

import sys
import logging
import numpy as np
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger("seed_real")

# ============================================================
# REAL anchor points from web search (Feb 20, 2026)
# ============================================================
# Fed Total Assets: $6,613B (WALCL, weekly, millions -> billions)
# TGA Balance: ~$949B (WTREGEN)
# ON RRP: ~$6-10B (was $106B on Dec 31, collapsed to ~$6B in Jan)
# SOFR: 3.67%
# HY OAS: 2.86%
# US 2Y: 3.48%
# US 10Y: 4.08%
# VIX: 19.09
# USD/JPY: 154.95
# JP 2Y: ~1.22%
# S&P 500: 6909.51
# DXY: 97.80
# BTC: ~$67,200  (peaked $126,198 in Oct 2025, now ~$67k)

def generate_real_calibrated_data():
    """Generate ~13 months of data ending at real Feb 20, 2026 levels."""
    np.random.seed(2026)
    dates = pd.bdate_range(start="2025-02-01", end="2026-02-20")
    n = len(dates)

    def path_to_target(start, end, vol, n, mean_revert=0.03):
        """Generate a mean-reverting path from start to end."""
        trend = np.linspace(start, end, n)
        noise = np.zeros(n)
        noise[0] = 0
        for i in range(1, n):
            noise[i] = noise[i-1] * (1 - mean_revert) + np.random.normal(0, vol)
        return trend + noise

    raw = {}

    # --- Fed Total Assets (FRED reports in millions, e.g. 6,613,000 = $6,613B) ---
    # Was ~6,800B early 2025, QT brought it to ~6,613B
    raw["fed_total_assets"] = pd.DataFrame(
        {"value": path_to_target(6810000, 6613000, 8000, n, 0.02)},
        index=dates
    )

    # --- TGA Balance (FRED reports in millions, e.g. 949,000 = $949B) ---
    # Volatile: ~700B early 2025, debt ceiling drama, now ~949B
    tga = path_to_target(720000, 949000, 30000, n, 0.04)
    # Add debt ceiling spike around mid-2025
    spike_center = n // 3
    tga[spike_center-10:spike_center+10] += np.linspace(0, 200000, 20)
    tga[spike_center+10:spike_center+30] += np.linspace(200000, 0, 20)
    tga = np.clip(tga, 300000, 1200000)
    raw["tga_balance"] = pd.DataFrame({"value": tga}, index=dates)

    # --- ON RRP (billions) ---
    # Was ~100-200B early 2025, declined to near zero, spike at year-end
    rrp = path_to_target(150, 8, 5, n, 0.05)
    rrp = np.clip(rrp, 2, 300)
    # Year-end spike (Dec 31)
    dec31_idx = None
    for i, d in enumerate(dates):
        if d.month == 12 and d.day >= 29:
            rrp[i] = max(rrp[i], 80 + np.random.normal(0, 10))
    raw["on_rrp"] = pd.DataFrame({"value": rrp}, index=dates)

    # --- SOFR (percent) ---
    # Was ~4.30-4.35% early 2025, Fed cut -> now 3.67%
    raw["sofr"] = pd.DataFrame(
        {"value": path_to_target(4.33, 3.67, 0.015, n, 0.05)},
        index=dates
    )

    # --- HY OAS (percent) ---
    # Was ~3.0-3.2% early 2025, tightened to ~2.86%
    # But had a stress spike around Apr 2025 (tariff fears)
    hy = path_to_target(3.10, 2.86, 0.06, n, 0.03)
    # Tariff shock spike around Apr 2025
    apr_idx = int(n * 0.15)
    hy[apr_idx:apr_idx+15] += np.linspace(0, 1.5, 15)
    hy[apr_idx+15:apr_idx+35] += np.linspace(1.5, 0, 20)
    hy = np.clip(hy, 2.0, 6.0)
    raw["hy_oas"] = pd.DataFrame({"value": hy}, index=dates)

    # --- US 2Y Yield (percent) ---
    # Was ~4.2% early 2025, declined with Fed cuts to 3.48%
    raw["us2y"] = pd.DataFrame(
        {"value": path_to_target(4.20, 3.48, 0.03, n, 0.02)},
        index=dates
    )

    # --- US 10Y Yield (percent) ---
    # Was ~4.5% early 2025, declined to 4.08%
    raw["us10y"] = pd.DataFrame(
        {"value": path_to_target(4.50, 4.08, 0.025, n, 0.02)},
        index=dates
    )

    # --- VIX ---
    # Range 13-60 over the year. Apr 7 spike to 60. Now 19.09
    vix = path_to_target(16, 19.09, 1.5, n, 0.08)
    # Apr 7 tariff spike (VIX 52wk high = 60.13)
    apr7_idx = int(n * 0.16)
    vix[apr7_idx-2:apr7_idx+1] = [45, 55, 60]
    vix[apr7_idx+1:apr7_idx+8] = np.linspace(55, 28, 7)
    vix[apr7_idx+8:apr7_idx+20] = np.linspace(28, 20, 12)
    # Dec low (13.38)
    dec_idx = int(n * 0.85)
    vix[dec_idx-3:dec_idx+3] = np.linspace(15, 13.4, 6)
    vix = np.clip(vix, 11, 65)
    raw["vix"] = pd.DataFrame({"value": vix}, index=dates)

    # --- USD/JPY ---
    # Was ~155-158 early 2025, hit 159.18 in Jan 2026, now 154.95
    usdjpy = path_to_target(155, 154.95, 1.0, n, 0.02)
    # Jul-Aug 2025 carry trade unwind -> drop to ~140-142
    jul_idx = int(n * 0.40)
    usdjpy[jul_idx:jul_idx+15] += np.linspace(0, -14, 15)
    usdjpy[jul_idx+15:jul_idx+40] += np.linspace(-14, 0, 25)
    # Jan 2026 peak 159.18
    jan26_idx = int(n * 0.90)
    usdjpy[jan26_idx:jan26_idx+5] += np.linspace(0, 4, 5)
    usdjpy[jan26_idx+5:jan26_idx+15] += np.linspace(4, 0, 10)
    raw["usdjpy"] = pd.DataFrame({"value": usdjpy}, index=dates)

    # --- JP 2Y Yield (sparse monthly) ---
    # Was ~0.35% early 2025, BOJ hiked, now ~1.22%
    jp_monthly = pd.bdate_range(start="2025-02-01", end="2026-02-20", freq="MS")
    jp_vals = np.linspace(0.35, 1.22, len(jp_monthly)) + np.random.normal(0, 0.03, len(jp_monthly))
    raw["jp2y"] = pd.DataFrame({"value": jp_vals}, index=jp_monthly)

    # --- S&P 500 ---
    # Was ~5900-6000 early 2025, hit ~7000 range, now 6909.51
    spx = path_to_target(5950, 6909.51, 35, n, 0.01)
    # Apr tariff crash
    apr_crash = int(n * 0.15)
    spx[apr_crash:apr_crash+10] += np.linspace(0, -600, 10)
    spx[apr_crash+10:apr_crash+40] += np.linspace(-600, 0, 30)
    raw["spx"] = pd.DataFrame({"value": spx}, index=dates)

    # --- DXY ---
    # Was ~108-110 early 2025, weakened significantly to 97.80
    raw["dxy"] = pd.DataFrame(
        {"value": path_to_target(108.5, 97.80, 0.6, n, 0.02)},
        index=dates
    )

    # --- BTC ---
    # Was ~$95,000 early 2025, peaked $126,198 in Oct 2025, crashed to ~$67,200
    btc = np.zeros(n)
    # Phase 1: $95k -> $126k (peak around Oct = ~0.65 of year)
    peak_idx = int(n * 0.65)
    btc[:peak_idx] = np.linspace(95000, 126198, peak_idx) + np.random.normal(0, 2000, peak_idx)
    # Phase 2: $126k -> $60k bottom -> $67k recovery
    bottom_idx = int(n * 0.88)
    btc[peak_idx:bottom_idx] = np.linspace(126198, 60000, bottom_idx - peak_idx) + np.random.normal(0, 1500, bottom_idx - peak_idx)
    btc[bottom_idx:] = np.linspace(60000, 67200, n - bottom_idx) + np.random.normal(0, 800, n - bottom_idx)
    btc = np.clip(btc, 40000, 135000)
    raw["btc"] = pd.DataFrame({"value": btc}, index=dates)

    for key in raw:
        raw[key].index.name = "date"

    return raw


def main():
    from run_daily import load_config

    logger.info("=" * 60)
    logger.info("REAL-CALIBRATED DATA PIPELINE")
    logger.info("=" * 60)

    config = load_config(PROJECT_ROOT / "config.yaml")
    config["cache"]["dir"] = str(PROJECT_ROOT / "cache")
    config["output"]["base_dir"] = str(PROJECT_ROOT / "output")

    raw_data = generate_real_calibrated_data()
    logger.info(f"Generated {len(raw_data)} indicators with real anchor points")

    from src.cleaner import DataCleaner
    cleaner = DataCleaner(config)
    panel = cleaner.build_daily_panel(raw_data)
    quality = cleaner.get_quality_report()

    from src.indicators import IndicatorEngine
    ind_engine = IndicatorEngine(config)
    panel = ind_engine.compute(panel)

    from src.signals import SignalEngine
    sig_engine = SignalEngine(config)
    signal_panel = sig_engine.compute(panel)

    from src.judge import JudgmentEngine
    judge = JudgmentEngine(config)
    judgment = judge.evaluate(panel, signal_panel, quality)

    logger.info(f"REGIME: {judgment['regime_cn']} ({judgment['regime']})")
    logger.info(f"Explanation: {judgment['explanation']}")

    output_dir = Path(config["output"]["base_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    panel.to_csv(output_dir / "daily_panel.csv")
    signal_panel.to_csv(output_dir / "signal_panel.csv")

    from src.summarizer import Summarizer
    summarizer = Summarizer(config)
    summary = summarizer.generate(panel, signal_panel, judgment, quality)

    from src.charter import ChartEngine
    charter = ChartEngine(config)
    chart_files = charter.generate_all(panel)
    logger.info(f"Generated {len(chart_files)} charts")

    from src.reporter import ReportGenerator
    reporter = ReportGenerator(config)
    reporter.generate(summary)

    from src.scorer import MacroScorer
    scorer = MacroScorer(config)
    score_data = scorer.compute(panel, signal_panel)
    logger.info(f"Score: {score_data['composite_score']} -> {score_data['tier_cn']}")

    from src.web_export import WebExporter
    exporter = WebExporter(config)
    exporter.export(summary, score_data)

    from src.dashboard import DashboardGenerator
    dashboard = DashboardGenerator(config)
    dashboard.generate(summary, score_data=score_data)

    print(f"\n{'='*50}")
    print(f"  SCORE: {score_data['composite_score']:.0f}/100 {score_data['tier_cn']}")
    print(f"  {judgment['regime_cn']} ({judgment['regime']})")
    print(f"  {judgment['explanation']}")
    print(f"{'='*50}")
    key_cols = ["net_liquidity", "sofr", "hy_oas", "vix", "usdjpy",
                "carry_spread_bps", "spx", "btc", "dxy"]
    for col in key_cols:
        if col in panel.columns:
            val = panel[col].dropna().iloc[-1]
            print(f"  {col:>25s}: {val:,.2f}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
