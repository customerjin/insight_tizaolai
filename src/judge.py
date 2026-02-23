"""
judge.py - Comprehensive judgment engine
Implements the 4 mandatory rules for liquidity regime classification.

Rules:
1. "明显趋紧" requires: net_liq weakening AND >= 2 confirmations
2. Single-indicator deterioration = "局部扰动"
3. If risk assets don't confirm = "前置信号已出现，但市场确认不足"
4. If data missing/stale = mark and give conservative judgment
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class JudgmentEngine:
    """
    Evaluate macro liquidity regime based on multi-indicator confirmation logic.
    """

    def __init__(self, config: dict):
        self.thresholds = config.get("judgment", {})
        self.data_quality = {}

    def evaluate(self, panel: pd.DataFrame, signal_panel: pd.DataFrame,
                 data_quality: dict) -> dict:
        """
        Main evaluation entry point.
        Returns a structured judgment dict.
        """
        self.data_quality = data_quality
        today = panel.index[-1] if len(panel) > 0 else None
        if today is None:
            return self._empty_judgment("No data available")

        # --- Step 1: Check data freshness ---
        stale_indicators = self._check_staleness(data_quality)

        # --- Step 2: Evaluate each dimension ---
        checks = {}

        # Net liquidity
        checks["net_liquidity"] = self._check_net_liquidity(panel, signal_panel)

        # SOFR (funding stress)
        checks["sofr"] = self._check_sofr(panel, signal_panel)

        # MOVE proxy / VIX (rate vol)
        checks["move_proxy"] = self._check_move_proxy(panel, signal_panel)

        # Carry trade chain (USDJPY + spread)
        checks["carry_chain"] = self._check_carry_chain(panel, signal_panel)

        # HY OAS (credit)
        checks["hy_oas"] = self._check_hy_oas(panel, signal_panel)

        # Risk asset confirmation
        checks["risk_assets"] = self._check_risk_assets(panel, signal_panel)

        # --- Step 3: Apply judgment rules ---
        judgment = self._apply_rules(checks, stale_indicators, today)

        return judgment

    # ----------------------------------------------------------------
    # Individual dimension checks
    # ----------------------------------------------------------------
    def _check_net_liquidity(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "weakening": False, "detail": ""}

        if "net_liquidity" not in panel.columns:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        latest = panel["net_liquidity"].dropna().iloc[-1] if panel["net_liquidity"].notna().any() else None
        if latest is None:
            result["detail"] = "all NaN"
            return result

        chg_5d_col = "net_liquidity_chg_5d"
        chg_20d_col = "net_liquidity_chg_20d"

        chg_5d = signal[chg_5d_col].dropna().iloc[-1] if chg_5d_col in signal.columns and signal[chg_5d_col].notna().any() else 0
        chg_20d = signal[chg_20d_col].dropna().iloc[-1] if chg_20d_col in signal.columns and signal[chg_20d_col].notna().any() else 0

        threshold = self.thresholds.get("net_liq_weak_threshold_5d", -50)
        result["level"] = round(float(latest), 1)
        result["chg_5d"] = round(float(chg_5d), 1)
        result["chg_20d"] = round(float(chg_20d), 1)
        result["weakening"] = bool(chg_5d < threshold)
        result["detail"] = f"Level: {result['level']}B, 5d: {result['chg_5d']}B, 20d: {result['chg_20d']}B"

        return result

    def _check_sofr(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "stress": False, "detail": ""}

        if "sofr" not in panel.columns:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        latest = self._last_valid(panel["sofr"])
        chg_5d = self._last_valid(signal.get("sofr_chg_5d"))

        if latest is None:
            result["detail"] = "all NaN"
            return result

        threshold = self.thresholds.get("sofr_stress_threshold_5d", 5) / 100  # bps -> pct
        result["level"] = round(float(latest), 4)
        result["chg_5d_bps"] = round(float(chg_5d * 100), 1) if chg_5d is not None else None
        result["stress"] = bool(chg_5d is not None and chg_5d > threshold)
        result["detail"] = f"SOFR: {result['level']}%, 5d chg: {result['chg_5d_bps']}bps"

        return result

    def _check_move_proxy(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "stress": False, "detail": ""}

        col = "move_proxy" if "move_proxy" in panel.columns else "vix"
        if col not in panel.columns:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        latest = self._last_valid(panel[col])

        if latest is None:
            result["detail"] = "all NaN"
            return result

        threshold = self.thresholds.get("vix_stress_threshold", 25)
        zscore = self._last_valid(signal.get(f"{col}_zscore"))

        result["level"] = round(float(latest), 1)
        result["zscore"] = round(float(zscore), 2) if zscore is not None else None
        result["stress"] = bool(latest > threshold or (zscore is not None and zscore > 1.0))
        result["detail"] = f"{col}: {result['level']}, z-score: {result['zscore']}"
        result["is_proxy"] = True

        return result

    def _check_carry_chain(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "stress": False, "detail": ""}

        has_usdjpy = "usdjpy" in panel.columns
        has_spread = "carry_spread_bps" in panel.columns

        if not has_usdjpy and not has_spread:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        sub_checks = []

        if has_usdjpy:
            usdjpy = self._last_valid(panel["usdjpy"])
            usdjpy_chg = self._last_valid(signal.get("usdjpy_chg_5d"))
            threshold = self.thresholds.get("usdjpy_stress_threshold_5d", -2.0)
            usdjpy_stress = bool(usdjpy_chg is not None and usdjpy_chg < threshold)
            sub_checks.append(f"USDJPY: {usdjpy:.1f}" if usdjpy else "USDJPY: N/A")
            if usdjpy_stress:
                sub_checks[-1] += " [STRESS]"
                result["stress"] = True

        if has_spread:
            spread = self._last_valid(panel["carry_spread_bps"])
            spread_chg = self._last_valid(signal.get("carry_spread_bps_chg_5d"))
            threshold = self.thresholds.get("carry_spread_narrow_threshold_5d", -10)
            spread_stress = bool(spread_chg is not None and spread_chg < threshold)
            sub_checks.append(f"US2Y-JP2Y: {spread:.0f}bps" if spread else "Spread: N/A")
            if spread_stress:
                sub_checks[-1] += " [STRESS]"
                result["stress"] = True

        result["detail"] = " | ".join(sub_checks)
        return result

    def _check_hy_oas(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "stress": False, "detail": ""}

        if "hy_oas" not in panel.columns:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        latest = self._last_valid(panel["hy_oas"])
        chg_5d = self._last_valid(signal.get("hy_oas_chg_5d"))

        if latest is None:
            result["detail"] = "all NaN"
            return result

        threshold_bps = self.thresholds.get("hy_oas_widen_threshold_5d", 15) / 100
        result["level"] = round(float(latest), 2)
        result["chg_5d"] = round(float(chg_5d * 100), 1) if chg_5d is not None else None
        result["stress"] = bool(chg_5d is not None and chg_5d > threshold_bps)
        result["detail"] = f"HY OAS: {result['level']}%, 5d: {result['chg_5d']}bps"

        return result

    def _check_risk_assets(self, panel: pd.DataFrame, signal: pd.DataFrame) -> dict:
        result = {"available": False, "confirming_weakness": False, "detail": ""}

        if "spx" not in panel.columns:
            result["detail"] = "data missing"
            return result

        result["available"] = True
        spx_pct = self._last_valid(signal.get("spx_pct_5d"))
        threshold = self.thresholds.get("spx_weak_threshold_5d", -0.02)

        btc_pct = self._last_valid(signal.get("btc_pct_5d"))

        detail_parts = []
        if spx_pct is not None:
            detail_parts.append(f"SPX 5d: {spx_pct*100:.1f}%")
        if btc_pct is not None:
            detail_parts.append(f"BTC 5d: {btc_pct*100:.1f}%")

        result["confirming_weakness"] = bool(spx_pct is not None and spx_pct < threshold)
        result["detail"] = " | ".join(detail_parts)

        return result

    # ----------------------------------------------------------------
    # Rule engine
    # ----------------------------------------------------------------
    def _apply_rules(self, checks: dict, stale: list, today) -> dict:
        """
        Apply the 4 mandatory judgment rules.
        """
        net_liq = checks.get("net_liquidity", {})
        risk = checks.get("risk_assets", {})

        # Count confirmation signals (excluding net_liquidity and risk_assets)
        confirmation_dims = ["sofr", "move_proxy", "carry_chain", "hy_oas"]
        stress_count = sum(1 for d in confirmation_dims if checks.get(d, {}).get("stress", False))
        stress_names = [d for d in confirmation_dims if checks.get(d, {}).get("stress", False)]

        min_conf = self.thresholds.get("min_confirmations", 2)

        # === Rule 4: Data staleness ===
        has_stale = len(stale) > 0
        stale_note = f"Data stale/missing for: {', '.join(stale)}" if has_stale else ""

        # === Rule 1: 明显趋紧 ===
        if net_liq.get("weakening") and stress_count >= min_conf:
            regime = "TIGHTENING"
            regime_cn = "明显趋紧"
            confidence = "high" if not has_stale else "medium"
            explanation = (
                f"净流动性走弱 ({net_liq.get('detail', '')})，"
                f"且 {stress_count} 个确认维度同步走弱 ({', '.join(stress_names)})"
            )

            # === Rule 3: Risk asset check ===
            if not risk.get("confirming_weakness", False):
                explanation += "。但风险资产尚未确认——前置信号已出现，市场确认不足"
                confidence = "medium" if confidence == "high" else "low"

        # === Rule 2: 局部扰动 ===
        elif stress_count > 0 or net_liq.get("weakening"):
            regime = "LOCAL_DISTURBANCE"
            regime_cn = "局部扰动"
            confidence = "medium" if not has_stale else "low"

            if net_liq.get("weakening"):
                explanation = f"净流动性走弱 ({net_liq.get('detail', '')})，但确认信号不足 ({stress_count}/{min_conf})"
            else:
                explanation = f"净流动性未显著走弱，但 {', '.join(stress_names)} 出现压力信号——属于局部扰动，不宜过度解读"

        else:
            regime = "STABLE"
            regime_cn = "流动性平稳"
            confidence = "high" if not has_stale else "medium"
            explanation = "净流动性与各确认维度均未触发走弱信号"

        # === Rule 4 annotation ===
        if has_stale:
            explanation += f"。[注意] {stale_note}，判断偏保守"
            if regime == "STABLE":
                confidence = "medium"

        return {
            "date": str(today.date()) if hasattr(today, 'date') else str(today),
            "regime": regime,
            "regime_cn": regime_cn,
            "confidence": confidence,
            "explanation": explanation,
            "net_liquidity_weakening": net_liq.get("weakening", False),
            "stress_count": stress_count,
            "stress_dimensions": stress_names,
            "risk_asset_confirming": risk.get("confirming_weakness", False),
            "data_stale": stale,
            "dimension_details": {k: v for k, v in checks.items()},
        }

    # ----------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------
    def _check_staleness(self, data_quality: dict) -> list:
        stale = []
        for key, info in data_quality.items():
            if isinstance(info, dict):
                if info.get("status") in ("missing", "degraded"):
                    stale.append(key)
                elif info.get("stale_days", 0) > 3:
                    stale.append(key)
        return stale

    def _last_valid(self, series) -> Optional[float]:
        if series is None:
            return None
        if isinstance(series, pd.Series) and series.notna().any():
            return float(series.dropna().iloc[-1])
        return None

    def _empty_judgment(self, reason: str) -> dict:
        return {
            "date": str(datetime.now().date()),
            "regime": "UNKNOWN",
            "regime_cn": "无法判断",
            "confidence": "none",
            "explanation": reason,
            "net_liquidity_weakening": False,
            "stress_count": 0,
            "stress_dimensions": [],
            "risk_asset_confirming": False,
            "data_stale": [],
            "dimension_details": {},
        }
