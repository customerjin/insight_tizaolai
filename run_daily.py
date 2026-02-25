#!/usr/bin/env python3
"""
run_daily.py - Unified entry point for Macro Liquidity Daily Monitor

Usage:
    python run_daily.py                    # Full run with defaults
    python run_daily.py --start 2024-01-01 # Custom start date
    python run_daily.py --no-charts        # Skip chart generation
    python run_daily.py --no-report        # Skip report generation
    python run_daily.py --clear-cache      # Clear cache before run
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Use built-in yaml parser or fallback
try:
    import yaml
except ImportError:
    yaml = None


def load_config(config_path: Path) -> dict:
    """Load YAML config, with fallback for missing PyYAML."""
    if yaml is not None:
        with open(config_path) as f:
            return yaml.safe_load(f)
    else:
        # Minimal YAML parser fallback for simple configs
        return _parse_yaml_simple(config_path)


def _parse_yaml_simple(config_path: Path) -> dict:
    """Bare-bones YAML parser for flat configs. For robust parsing, install PyYAML."""
    import re
    config = {}
    current_section = config
    section_stack = [(0, config)]

    with open(config_path) as f:
        for line in f:
            stripped = line.rstrip()
            if not stripped or stripped.lstrip().startswith("#"):
                continue

            indent = len(line) - len(line.lstrip())
            content = stripped.strip()

            if content.endswith(":") and not ":" in content[:-1]:
                key = content[:-1].strip()
                new_dict = {}
                # Find parent based on indent
                while len(section_stack) > 1 and section_stack[-1][0] >= indent:
                    section_stack.pop()
                section_stack[-1][1][key] = new_dict
                section_stack.append((indent, new_dict))
            elif ":" in content:
                key, val = content.split(":", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")

                # Type inference
                if val == "":
                    val = ""
                elif val.lower() in ("true", "false"):
                    val = val.lower() == "true"
                elif re.match(r"^-?\d+$", val):
                    val = int(val)
                elif re.match(r"^-?\d+\.\d+$", val):
                    val = float(val)
                elif val.startswith("[") and val.endswith("]"):
                    val = [int(x.strip()) for x in val[1:-1].split(",") if x.strip()]

                while len(section_stack) > 1 and section_stack[-1][0] >= indent:
                    section_stack.pop()
                section_stack[-1][1][key] = val

    return config


def setup_logging(config: dict) -> logging.Logger:
    """Configure logging to both file and console."""
    log_dir = Path(PROJECT_ROOT / config.get("logging", {}).get("dir", "output/logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_level = getattr(logging, config.get("logging", {}).get("level", "INFO"))

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                                            datefmt="%H:%M:%S"))
    root_logger.addHandler(console)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root_logger.addHandler(file_handler)

    logging.info(f"Log file: {log_file}")
    return root_logger


def run_daily_brief(config: dict, macro_data: dict = None, output_dir: Path = None) -> dict:
    """Run the daily brief module. Returns brief data dict."""
    logger = logging.getLogger("daily_brief")
    try:
        from services.brief_service import BriefService

        brief_service = BriefService(config)
        brief_data = brief_service.generate(macro_data=macro_data)

        # Save brief JSON
        if output_dir:
            import json
            brief_dir = output_dir / "brief"
            brief_dir.mkdir(parents=True, exist_ok=True)

            brief_path = brief_dir / "daily_brief.json"
            with open(brief_path, 'w', encoding='utf-8') as f:
                json.dump(brief_data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Daily brief saved: {brief_path}")

            # Also merge into web/latest.json if it exists
            web_json_path = output_dir / "web" / "latest.json"
            if web_json_path.exists():
                try:
                    with open(web_json_path, 'r', encoding='utf-8') as f:
                        web_data = json.load(f)
                    web_data['daily_brief'] = brief_data
                    with open(web_json_path, 'w', encoding='utf-8') as f:
                        json.dump(web_data, f, ensure_ascii=False, default=str)
                    logger.info(f"Merged brief into {web_json_path}")
                except Exception as e:
                    logger.warning(f"Failed to merge brief into web JSON: {e}")

        return brief_data
    except Exception as e:
        logger.error(f"Daily brief generation failed: {e}", exc_info=True)
        return {'status': 'error', 'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description="Macro Liquidity Daily Monitor")
    parser.add_argument("--start", default="2024-01-01", help="Data start date (YYYY-MM-DD)")
    parser.add_argument("--no-charts", action="store_true", help="Skip chart generation")
    parser.add_argument("--no-report", action="store_true", help="Skip report generation")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cache before run")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--no-brief", action="store_true", help="Skip daily brief generation")
    parser.add_argument("--brief-only", action="store_true", help="Only run daily brief (skip macro pipeline)")
    args = parser.parse_args()

    # ---- Load Config ----
    config_path = PROJECT_ROOT / args.config
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    config = load_config(config_path)

    # Override paths to be relative to project root
    cache_dir = PROJECT_ROOT / config.get("cache", {}).get("dir", "cache")
    output_dir = PROJECT_ROOT / config.get("output", {}).get("base_dir", "output")
    config.setdefault("cache", {})["dir"] = str(cache_dir)
    config.setdefault("output", {})["base_dir"] = str(output_dir)

    # ---- Setup Logging ----
    setup_logging(config)
    logger = logging.getLogger("main")
    logger.info("=" * 60)
    logger.info("MACRO LIQUIDITY DAILY MONITOR")
    logger.info(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ---- Clear Cache ----
    if args.clear_cache:
        import shutil
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            logger.info("Cache cleared")

    # ---- Brief-only mode ----
    if args.brief_only:
        logger.info("Running daily brief only (skipping macro pipeline)...")
        brief_data = run_daily_brief(config, macro_data=None, output_dir=output_dir)
        logger.info("Brief-only run complete")
        return

    # ---- Phase 1: Fetch Data ----
    logger.info("Phase 1: Fetching data...")
    from src.fetcher import DataFetcher
    fetcher = DataFetcher(config)
    raw_data = fetcher.fetch_all(start_date=args.start)
    logger.info(fetcher.get_fetch_report())

    if not raw_data:
        logger.error("No data fetched. Aborting.")
        sys.exit(1)

    # ---- Phase 2: Clean & Align ----
    logger.info("Phase 2: Cleaning and aligning data...")
    from src.cleaner import DataCleaner
    cleaner = DataCleaner(config)
    daily_panel = cleaner.build_daily_panel(raw_data)
    data_quality = cleaner.get_quality_report()
    logger.info(f"Daily panel: {daily_panel.shape}")

    # ---- Phase 3: Compute Indicators ----
    logger.info("Phase 3: Computing derived indicators...")
    from src.indicators import IndicatorEngine
    indicator_engine = IndicatorEngine(config)
    daily_panel = indicator_engine.compute(daily_panel)

    # ---- Phase 4: Compute Signals ----
    logger.info("Phase 4: Computing signals...")
    from src.signals import SignalEngine
    signal_engine = SignalEngine(config)
    signal_panel = signal_engine.compute(daily_panel)

    # ---- Phase 5: Judgment ----
    logger.info("Phase 5: Running judgment engine...")
    from src.judge import JudgmentEngine
    judge = JudgmentEngine(config)
    judgment = judge.evaluate(daily_panel, signal_panel, data_quality)

    logger.info(f"REGIME: {judgment['regime_cn']} ({judgment['regime']})")
    logger.info(f"Confidence: {judgment['confidence']}")
    logger.info(f"Explanation: {judgment['explanation']}")

    # ---- Phase 6: Save Outputs ----
    logger.info("Phase 6: Saving outputs...")
    output_dir.mkdir(parents=True, exist_ok=True)

    # daily_panel.csv
    panel_path = output_dir / "daily_panel.csv"
    daily_panel.to_csv(panel_path)
    logger.info(f"Saved: {panel_path}")

    # signal_panel.csv
    signal_path = output_dir / "signal_panel.csv"
    signal_panel.to_csv(signal_path)
    logger.info(f"Saved: {signal_path}")

    # ---- Phase 7: Summary ----
    logger.info("Phase 7: Generating summary...")
    from src.summarizer import Summarizer
    summarizer = Summarizer(config)
    summary = summarizer.generate(daily_panel, signal_panel, judgment, data_quality)

    # ---- Phase 8: Charts ----
    if not args.no_charts:
        logger.info("Phase 8: Generating charts...")
        from src.charter import ChartEngine
        charter = ChartEngine(config)
        chart_files = charter.generate_all(daily_panel)
        logger.info(f"Generated {len(chart_files)} charts")
    else:
        logger.info("Phase 8: Charts skipped (--no-charts)")

    # ---- Phase 9: Report ----
    if not args.no_report:
        logger.info("Phase 9: Generating report...")
        from src.reporter import ReportGenerator
        reporter = ReportGenerator(config)
        report_text = reporter.generate(summary)
        logger.info("Report generated")
    else:
        logger.info("Phase 9: Report skipped (--no-report)")

    # ---- Phase 10: Composite Score ----
    logger.info("Phase 10: Computing composite liquidity score...")
    from src.scorer import MacroScorer
    scorer = MacroScorer(config)
    score_data = scorer.compute(daily_panel, signal_panel)
    logger.info(f"Score: {score_data['composite_score']:.0f}/100 -> {score_data['tier_cn']}")

    # ---- Phase 11: Web JSON Export ----
    logger.info("Phase 11: Exporting web JSON...")
    from src.web_export import WebExporter
    exporter = WebExporter(config)
    exporter.export(summary, score_data)

    # ---- Phase 12: HTML Dashboard ----
    if not args.no_charts:
        logger.info("Phase 12: Generating HTML dashboard...")
        from src.dashboard import DashboardGenerator
        dashboard = DashboardGenerator(config)
        dashboard_path = dashboard.generate(summary, score_data=score_data)
        logger.info(f"Dashboard: {dashboard_path}")
    else:
        dashboard_path = None
        logger.info("Phase 12: Dashboard skipped (no charts)")

    # ---- Phase 13: Daily Brief ----
    brief_data = None
    if not args.no_brief:
        logger.info("Phase 13: Generating daily brief...")
        macro_context = {
            'score': {
                'composite': score_data.get('composite_score'),
                'tier': score_data.get('tier'),
                'tier_cn': score_data.get('tier_cn'),
            },
            'judgment': judgment,
        }
        brief_data = run_daily_brief(config, macro_data=macro_context, output_dir=output_dir)
    else:
        logger.info("Phase 13: Daily brief skipped (--no-brief)")

    # ---- Done ----
    logger.info("=" * 60)
    logger.info("RUN COMPLETE")
    logger.info(f"Regime: {judgment['regime_cn']} ({judgment['regime']})")
    logger.info(f"Output: {output_dir}")
    logger.info("=" * 60)

    # Print summary to stdout
    print(f"\n{'='*50}")
    print(f"  {judgment['regime_cn']} ({judgment['regime']})")
    print(f"  Confidence: {judgment['confidence']}")
    print(f"  {judgment['explanation']}")
    print(f"{'='*50}")
    print(f"  Output: {output_dir}/")
    print(f"  - daily_panel.csv  ({daily_panel.shape[0]} days x {daily_panel.shape[1]} cols)")
    print(f"  - signal_panel.csv ({signal_panel.shape[0]} days x {signal_panel.shape[1]} cols)")
    print(f"  - summary_for_llm.json")
    if not args.no_charts:
        print(f"  - charts/ ({len(chart_files)} files)")
    if not args.no_report:
        print(f"  - daily_report.md")
    if dashboard_path:
        print(f"  - dashboard.html (open in browser)")
    if brief_data and brief_data.get('status') != 'error':
        print(f"  - brief/daily_brief.json (daily analysis)")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
