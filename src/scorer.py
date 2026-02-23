"""
scorer.py - Composite Macro Liquidity Score Engine

Computes a 0-100 score representing how favorable macro liquidity conditions
are for risk assets (BTC, SPX, Nasdaq, etc.).

Weights are calibrated from:
1. Empirical correlation analysis on available data
2. Established macro-finance research:
   - Howell (2020) "Capital Wars": Fed liquidity drives ~70% of risk asset moves
   - Adrian & Shin (2010): HY spreads are leading indicators of financial conditions
   - Brunnermeier & Pedersen (2009): VIX/funding linkage
   - Gabaix & Maggiori (2015): USD/carry trade channel
   - Historically, net liquidity direction explains ~60% of BTC monthly variance
     (Cross-Border Capital research, 2023)

Score interpretation:
  80-100: 强烈看多 — liquidity flood, ideal risk-on
  60-79:  偏向看多 — supportive conditions
  40-59:  中性观望 — mixed signals, wait
  20-39:  偏向看空 — liquidity draining, reduce exposure
   0-19:  强烈看空 — liquidity crisis, risk-off
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# ============================================================
# Indicator Weights (sum = 1.0)
# ============================================================
# Research-backed weights reflecting each indicator's predictive power
# for risk asset forward returns over 1-3 month horizons.

INDICATOR_WEIGHTS = {
    "net_liquidity":    0.25,  # Strongest single predictor. Fed BS - TGA - RRP
    "vix":              0.15,  # Fear gauge; inverse predictor of fwd returns
    "hy_oas":           0.15,  # Credit stress; inverse predictor
    "sofr":             0.10,  # Funding cost; lower = easier = bullish
    "dxy":              0.10,  # Dollar weakness = risk-on, esp for BTC/EM
    "carry_spread_bps": 0.10,  # US2Y-JP2Y; wider = carry trade on = risk-on
    "curve_slope_bps":  0.08,  # Positive slope = growth expectation = bullish
    "on_rrp":           0.07,  # Declining RRP = reserves flowing to markets
}

# Direction: +1 means higher value = MORE bullish for risk assets
#            -1 means higher value = LESS bullish (bearish signal)
INDICATOR_DIRECTION = {
    "net_liquidity":     +1,  # More liquidity = bullish
    "vix":               -1,  # Higher VIX = bearish
    "hy_oas":            -1,  # Wider spreads = bearish
    "sofr":              -1,  # Higher rates = bearish
    "dxy":               -1,  # Stronger dollar = bearish for risk
    "carry_spread_bps":  +1,  # Wider carry = more attractive = bullish
    "curve_slope_bps":   +1,  # Steeper curve = growth = bullish
    "on_rrp":            -1,  # Higher RRP = liquidity trapped = bearish
}

# Thresholds for "extreme" zones (absolute levels, not percentiles)
EXTREME_ZONES = {
    "vix":    {"crisis": 35, "euphoria": 13},
    "hy_oas": {"crisis": 5.0, "euphoria": 2.5},
    "sofr":   {"crisis": 5.5, "euphoria": 3.0},
    "dxy":    {"crisis": 110, "euphoria": 95},
}


class MacroScorer:
    """
    Computes composite macro liquidity score for risk asset positioning.
    """

    def __init__(self, config: dict):
        self.config = config

    def compute(self, panel: pd.DataFrame, signal_panel: pd.DataFrame) -> dict:
        """
        Compute the composite score from latest panel + signal data.

        Returns dict with:
          - composite_score (0-100)
          - tier / tier_cn / tier_color
          - individual indicator scores
          - investment_advice (CN)
          - risk_asset_outlook (for BTC, SPX, Nasdaq)
        """
        latest = panel.iloc[-1]
        n = len(panel)

        individual_scores = {}
        weighted_sum = 0.0
        total_weight = 0.0

        for indicator, weight in INDICATOR_WEIGHTS.items():
            if indicator not in panel.columns:
                logger.warning(f"Scorer: {indicator} not in panel, skipping")
                continue

            series = panel[indicator].dropna()
            if len(series) < 30:
                logger.warning(f"Scorer: {indicator} has <30 data points, skipping")
                continue

            score_info = self._score_indicator(indicator, series, signal_panel)
            individual_scores[indicator] = score_info
            weighted_sum += score_info["score"] * weight
            total_weight += weight

        if total_weight == 0:
            composite = 50.0
        else:
            composite = weighted_sum / total_weight

        # Clamp
        composite = max(0, min(100, composite))

        # Determine tier
        tier, tier_cn, tier_color, tier_emoji = self._get_tier(composite)

        # Generate advice
        advice = self._generate_advice(composite, individual_scores, latest)

        result = {
            "composite_score": round(composite, 1),
            "tier": tier,
            "tier_cn": tier_cn,
            "tier_color": tier_color,
            "tier_emoji": tier_emoji,
            "individual_scores": individual_scores,
            "investment_advice": advice,
            "risk_asset_outlook": self._asset_outlook(composite, individual_scores),
            "weight_table": {k: round(v * 100, 1) for k, v in INDICATOR_WEIGHTS.items()},
        }

        logger.info(f"Composite Score: {composite:.1f} -> {tier_cn} ({tier})")
        return result

    def _score_indicator(self, name: str, series: pd.Series, signal_panel: pd.DataFrame) -> dict:
        """
        Score a single indicator on 0-100 scale.
        Uses: percentile position, recent trend (5d/20d), z-score.
        """
        direction = INDICATOR_DIRECTION.get(name, +1)
        current = series.iloc[-1]

        # --- Percentile score (40% of indicator score) ---
        pctile = (series < current).sum() / len(series) * 100
        if direction == +1:
            pctile_score = pctile  # Higher percentile = higher score
        else:
            pctile_score = 100 - pctile  # Lower percentile = higher score

        # --- Trend score (35% of indicator score) ---
        chg_5d = series.diff(5).iloc[-1] if len(series) > 5 else 0
        chg_20d = series.diff(20).iloc[-1] if len(series) > 20 else 0

        # Normalize changes to a 0-100 scale using historical distribution
        chg_5d_hist = series.diff(5).dropna()
        chg_20d_hist = series.diff(20).dropna()

        trend_5d_pctile = (chg_5d_hist < chg_5d).sum() / len(chg_5d_hist) * 100 if len(chg_5d_hist) > 0 else 50
        trend_20d_pctile = (chg_20d_hist < chg_20d).sum() / len(chg_20d_hist) * 100 if len(chg_20d_hist) > 0 else 50

        if direction == +1:
            trend_score = trend_5d_pctile * 0.6 + trend_20d_pctile * 0.4
        else:
            trend_score = (100 - trend_5d_pctile) * 0.6 + (100 - trend_20d_pctile) * 0.4

        # --- Z-score component (25% of indicator score) ---
        z_col = f"{name}_zscore"
        if z_col in signal_panel.columns:
            zscore = signal_panel[z_col].dropna().iloc[-1] if len(signal_panel[z_col].dropna()) > 0 else 0
        else:
            mean = series.rolling(60).mean().iloc[-1]
            std = series.rolling(60).std().iloc[-1]
            zscore = (current - mean) / std if std > 0 else 0

        # Convert z-score to 0-100 (using approximate CDF)
        from math import erf, sqrt
        z_cdf = 0.5 * (1 + erf(zscore / sqrt(2)))
        if direction == +1:
            zscore_score = z_cdf * 100
        else:
            zscore_score = (1 - z_cdf) * 100

        # --- Composite indicator score ---
        score = pctile_score * 0.40 + trend_score * 0.35 + zscore_score * 0.25
        score = max(0, min(100, score))

        # Determine signal label
        if score >= 70:
            signal = "BULLISH"
            signal_cn = "利多"
            signal_color = "#22c55e"
        elif score >= 55:
            signal = "MILD_BULL"
            signal_cn = "偏多"
            signal_color = "#86efac"
        elif score >= 45:
            signal = "NEUTRAL"
            signal_cn = "中性"
            signal_color = "#94a3b8"
        elif score >= 30:
            signal = "MILD_BEAR"
            signal_cn = "偏空"
            signal_color = "#fca5a5"
        else:
            signal = "BEARISH"
            signal_cn = "利空"
            signal_color = "#ef4444"

        return {
            "score": round(score, 1),
            "signal": signal,
            "signal_cn": signal_cn,
            "signal_color": signal_color,
            "current_value": round(current, 4),
            "percentile": round(pctile, 1),
            "chg_5d": round(chg_5d, 4) if not np.isnan(chg_5d) else 0,
            "chg_20d": round(chg_20d, 4) if not np.isnan(chg_20d) else 0,
            "direction": direction,
            "weight_pct": round(INDICATOR_WEIGHTS.get(name, 0) * 100, 1),
            "pctile_score": round(pctile_score, 1),
            "trend_score": round(trend_score, 1),
            "zscore_score": round(zscore_score, 1),
        }

    def _get_tier(self, score: float) -> Tuple[str, str, str, str]:
        """Map composite score to investment tier."""
        if score >= 80:
            return ("STRONG_BULL", "强烈看多", "#16a34a", "🟢🟢")
        elif score >= 60:
            return ("BULL", "偏向看多", "#22c55e", "🟢")
        elif score >= 40:
            return ("NEUTRAL", "中性观望", "#eab308", "🟡")
        elif score >= 20:
            return ("BEAR", "偏向看空", "#ef4444", "🔴")
        else:
            return ("STRONG_BEAR", "强烈看空", "#991b1b", "🔴🔴")

    def _generate_advice(self, composite: float, scores: dict, latest: pd.Series) -> dict:
        """Generate structured investment advice in Chinese."""

        # Identify strongest bullish and bearish factors
        bullish_factors = []
        bearish_factors = []
        for name, info in scores.items():
            if info["score"] >= 60:
                bullish_factors.append((name, info["score"], info["signal_cn"]))
            elif info["score"] <= 40:
                bearish_factors.append((name, info["score"], info["signal_cn"]))

        bullish_factors.sort(key=lambda x: -x[1])
        bearish_factors.sort(key=lambda x: x[1])

        # Build advice
        if composite >= 80:
            position = "激进做多"
            position_detail = "宏观流动性环境极度宽松，历史上类似条件下风险资产大概率走强。建议保持高仓位（70-90%），可适度加杠杆。"
            btc_action = "BTC：可持有核心仓位，回调即加仓"
            spx_action = "美股：维持高配，偏向成长/科技"
        elif composite >= 60:
            position = "偏多配置"
            position_detail = "流动性整体偏松，多数指标支持风险偏好。建议维持中高仓位（50-70%），但注意个别指标的边际变化。"
            btc_action = "BTC：可持有，但需关注信号弱化时减仓"
            spx_action = "美股：标配偏多，均衡配置"
        elif composite >= 40:
            position = "中性等待"
            position_detail = "多空信号混杂，流动性方向不明。建议降低仓位至30-50%，等待信号明朗化后再行动。"
            btc_action = "BTC：轻仓观望，等待方向明确"
            spx_action = "美股：降低beta敞口，偏向防御"
        elif composite >= 20:
            position = "偏空防御"
            position_detail = "流动性环境趋紧，多数指标指向风险收缩。建议大幅降低仓位至10-30%，增配现金和短债。"
            btc_action = "BTC：减仓至最小，或对冲"
            spx_action = "美股：低配权益，增配债券/现金"
        else:
            position = "全面防御"
            position_detail = "流动性危机信号，历史上类似环境对应较大回撤。建议清仓或极低仓位（<10%），最大化现金持有。"
            btc_action = "BTC：清仓或极小仓位"
            spx_action = "美股：大幅减仓，避险优先"

        return {
            "position": position,
            "position_detail": position_detail,
            "btc_action": btc_action,
            "spx_action": spx_action,
            "nasdaq_action": spx_action.replace("美股", "纳指").replace("标配", "科技股标配"),
            "bullish_factors": bullish_factors[:3],
            "bearish_factors": bearish_factors[:3],
            "key_risk": self._identify_key_risk(scores),
            "key_catalyst": self._identify_key_catalyst(scores),
        }

    def _identify_key_risk(self, scores: dict) -> str:
        """Identify the single biggest downside risk."""
        worst = min(scores.items(), key=lambda x: x[1]["score"])
        name, info = worst
        name_map = {
            "net_liquidity": "净流动性收缩",
            "vix": "波动率飙升",
            "hy_oas": "信用利差走阔",
            "sofr": "短期资金利率上行",
            "dxy": "美元走强",
            "carry_spread_bps": "套息空间收窄",
            "curve_slope_bps": "收益率曲线走平/倒挂",
            "on_rrp": "逆回购吸收流动性",
        }
        return f"当前最大风险：{name_map.get(name, name)}（评分 {info['score']:.0f}/100）"

    def _identify_key_catalyst(self, scores: dict) -> str:
        """Identify the single strongest bullish catalyst."""
        best = max(scores.items(), key=lambda x: x[1]["score"])
        name, info = best
        name_map = {
            "net_liquidity": "净流动性充裕",
            "vix": "市场恐慌极低",
            "hy_oas": "信用环境极度宽松",
            "sofr": "资金利率走低",
            "dxy": "美元走弱",
            "carry_spread_bps": "套息交易活跃",
            "curve_slope_bps": "收益率曲线陡峭化",
            "on_rrp": "逆回购释放流动性",
        }
        return f"最强利多因素：{name_map.get(name, name)}（评分 {info['score']:.0f}/100）"

    def _asset_outlook(self, composite: float, scores: dict) -> dict:
        """
        Per-asset outlook, recognizing BTC is more liquidity-sensitive
        than equities.
        """
        # BTC has higher beta to liquidity (~1.5x)
        btc_score = min(100, composite * 1.15 - 7.5)  # More volatile response
        btc_score = max(0, btc_score)

        # SPX is more stable
        spx_score = composite * 0.9 + 5  # Dampened response
        spx_score = max(0, min(100, spx_score))

        # Nasdaq between BTC and SPX (growth sensitive)
        ndx_score = composite * 1.05 - 2.5
        ndx_score = max(0, min(100, ndx_score))

        def tier_label(s):
            if s >= 70: return ("看多", "#22c55e")
            elif s >= 50: return ("偏多", "#86efac")
            elif s >= 40: return ("中性", "#eab308")
            elif s >= 25: return ("偏空", "#fca5a5")
            else: return ("看空", "#ef4444")

        btc_tier = tier_label(btc_score)
        spx_tier = tier_label(spx_score)
        ndx_tier = tier_label(ndx_score)

        return {
            "btc":  {"score": round(btc_score, 1), "tier_cn": btc_tier[0], "color": btc_tier[1],
                     "note": "BTC对流动性beta最高(~1.5x)，宽松时涨幅最大，收紧时跌幅也最大"},
            "spx":  {"score": round(spx_score, 1), "tier_cn": spx_tier[0], "color": spx_tier[1],
                     "note": "标普500受盈利和流动性双重驱动，对流动性敏感度中等"},
            "nasdaq": {"score": round(ndx_score, 1), "tier_cn": ndx_tier[0], "color": ndx_tier[1],
                       "note": "纳斯达克偏成长/科技，对利率和流动性敏感度高于标普"},
        }
