"""
indicators.py - Derived indicator calculations
Computes: net liquidity, carry spread, MOVE proxy, curve slope, etc.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class IndicatorEngine:
    """Calculate derived indicators from the daily panel."""

    def __init__(self, config: dict):
        self.config = config

    def compute(self, panel: pd.DataFrame) -> pd.DataFrame:
        """Add all derived indicators to the panel. Returns augmented panel."""
        df = panel.copy()

        # 1. Net Liquidity = Fed Assets - TGA - ON RRP (all in billions)
        df = self._compute_net_liquidity(df)

        # 2. US2Y - JP2Y carry spread
        df = self._compute_carry_spread(df)

        # 3. MOVE proxy (VIX-based rate volatility approximation)
        df = self._compute_move_proxy(df)

        # 4. Yield curve slope (10Y - 2Y)
        df = self._compute_curve_slope(df)

        # 5. SPX daily return (for risk confirmation)
        df = self._compute_returns(df)

        logger.info(f"Indicators computed. Panel now: {df.shape[1]} columns")
        return df

    def _compute_net_liquidity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Net Liquidity = Fed Total Assets - TGA - ON RRP"""
        required = ["fed_total_assets", "tga_balance", "on_rrp"]
        if all(col in df.columns for col in required):
            df["net_liquidity"] = df["fed_total_assets"] - df["tga_balance"] - df["on_rrp"]
            logger.info("Computed: net_liquidity")
        else:
            missing = [c for c in required if c not in df.columns]
            logger.warning(f"Cannot compute net_liquidity, missing: {missing}")
        return df

    def _compute_carry_spread(self, df: pd.DataFrame) -> pd.DataFrame:
        """Carry spread = US 2Y - JP 2Y (in bps)"""
        if "us2y" in df.columns and "jp2y" in df.columns:
            df["carry_spread_bps"] = (df["us2y"] - df["jp2y"]) * 100
            logger.info("Computed: carry_spread_bps")
        else:
            logger.warning("Cannot compute carry_spread_bps")
        return df

    def _compute_move_proxy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        MOVE proxy: Use VIX as base, scale by recent rate volatility.
        MOVE_proxy = VIX * (1 + normalized_rate_vol)
        where rate_vol = rolling 20d std of US10Y yield changes.
        """
        if "vix" in df.columns and "us10y" in df.columns:
            rate_changes = df["us10y"].diff()
            rate_vol = rate_changes.rolling(20, min_periods=5).std()
            # Normalize rate vol to [0, 1] range using recent history
            rate_vol_norm = (rate_vol - rate_vol.rolling(252, min_periods=60).mean()) / \
                           rate_vol.rolling(252, min_periods=60).std()
            rate_vol_norm = rate_vol_norm.clip(-2, 2)  # Bound extreme values

            df["move_proxy"] = df["vix"] * (1 + 0.3 * rate_vol_norm.fillna(0))
            logger.info("Computed: move_proxy (VIX * rate_vol scaled)")
        elif "vix" in df.columns:
            df["move_proxy"] = df["vix"]  # Fallback: just use VIX
            logger.warning("move_proxy = VIX (no rate vol adjustment)")
        else:
            logger.warning("Cannot compute move_proxy: VIX missing")
        return df

    def _compute_curve_slope(self, df: pd.DataFrame) -> pd.DataFrame:
        """Yield curve slope = 10Y - 2Y (in bps)"""
        if "us10y" in df.columns and "us2y" in df.columns:
            df["curve_slope_bps"] = (df["us10y"] - df["us2y"]) * 100
            logger.info("Computed: curve_slope_bps")
        return df

    def _compute_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute daily returns for risk assets."""
        for col in ["spx", "btc"]:
            if col in df.columns:
                df[f"{col}_ret_1d"] = df[col].pct_change()
                logger.info(f"Computed: {col}_ret_1d")
        return df
