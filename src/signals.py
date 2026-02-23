"""
signals.py - Signal calculation engine
Computes: period changes, z-scores, percentiles, signal labels.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class SignalEngine:
    """Compute signals, z-scores, percentiles, and labels from the indicator panel."""

    def __init__(self, config: dict):
        self.signal_cfg = config.get("signal", {})
        self.change_windows = self.signal_cfg.get("change_windows", [1, 5, 20])
        self.zscore_window = self.signal_cfg.get("zscore_window", 60)
        self.percentile_window = self.signal_cfg.get("percentile_window", 252)

    # Key indicators to track signals for
    SIGNAL_INDICATORS = [
        "net_liquidity", "sofr", "hy_oas", "vix", "move_proxy",
        "usdjpy", "carry_spread_bps", "curve_slope_bps",
        "us2y", "us10y", "spx", "btc", "dxy",
    ]

    def compute(self, panel: pd.DataFrame) -> pd.DataFrame:
        """Build the signal panel with changes, z-scores, percentiles, and labels."""
        all_series = {}

        for col in self.SIGNAL_INDICATORS:
            if col not in panel.columns:
                continue

            series = panel[col]

            # Copy raw value
            all_series[f"{col}_level"] = series

            # Period changes
            for w in self.change_windows:
                all_series[f"{col}_chg_{w}d"] = series.diff(w)
                if col in ("spx", "btc", "usdjpy", "dxy"):
                    all_series[f"{col}_pct_{w}d"] = series.pct_change(w)

            # Rolling z-score
            rolling_mean = series.rolling(self.zscore_window, min_periods=20).mean()
            rolling_std = series.rolling(self.zscore_window, min_periods=20).std()
            zscore = (series - rolling_mean) / rolling_std.replace(0, np.nan)
            all_series[f"{col}_zscore"] = zscore

            # Rolling percentile
            all_series[f"{col}_pctl"] = series.rolling(
                self.percentile_window, min_periods=60
            ).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)

            # Signal labels
            chg_5d = all_series.get(f"{col}_chg_5d")
            all_series[f"{col}_signal"] = self._label_signal(zscore, chg_5d, col)

        signal_df = pd.concat(all_series, axis=1)
        signal_df.index.name = "date"
        logger.info(f"Signal panel: {signal_df.shape[1]} columns")
        return signal_df

    def _label_signal(self, zscore: pd.Series, chg_5d: pd.Series, indicator: str) -> pd.Series:
        """
        Generate signal labels: TIGHT / EASING / NEUTRAL / STRESS
        Direction depends on indicator type.
        """
        labels = pd.Series("NEUTRAL", index=zscore.index)

        # Indicators where higher = tighter liquidity
        tightening_up = ["sofr", "hy_oas", "vix", "move_proxy", "dxy"]
        # Indicators where lower = tighter liquidity
        tightening_down = ["net_liquidity", "usdjpy", "carry_spread_bps", "spx", "btc"]

        if indicator in tightening_up:
            labels[zscore > 1.5] = "STRESS"
            labels[(zscore > 0.5) & (zscore <= 1.5)] = "TIGHT"
            labels[zscore < -0.5] = "EASING"
        elif indicator in tightening_down:
            labels[zscore < -1.5] = "STRESS"
            labels[(zscore < -0.5) & (zscore >= -1.5)] = "TIGHT"
            labels[zscore > 0.5] = "EASING"
        else:
            # Default: extreme both ways
            labels[zscore.abs() > 1.5] = "STRESS"
            labels[(zscore.abs() > 0.5) & (zscore.abs() <= 1.5)] = "TIGHT"

        return labels
