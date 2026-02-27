"""
forward_analyzer.py - Forward-Looking Macro Analysis Engine

The macro dashboard shows current/lagging indicators. This module adds
forward-looking intelligence by:

1. TREND ANALYSIS: Direction, velocity, acceleration of each indicator
   - Is the trend accelerating or decelerating?
   - What's the momentum (rate of change of rate of change)?

2. HISTORICAL PATTERN MATCHING (backtesting):
   - Find past periods with similar score profiles
   - Measure what happened to risk assets (SPX, BTC) after those periods
   - Calculate forward return distributions: median, 25th/75th percentile

3. AI-POWERED FORWARD SYNTHESIS:
   - Pass all quantitative signals to LLM
   - Generate a forward-looking narrative in Chinese
   - Output: bias, confidence, horizon, expected ranges

The output feeds into latest.json → displayed in Macro Dashboard tab.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Indicator names in Chinese for AI prompt
IND_NAME_CN = {
    "net_liquidity": "美联储净流动性",
    "vix": "VIX恐慌指数",
    "hy_oas": "高收益债利差",
    "sofr": "SOFR隔夜融资利率",
    "dxy": "美元指数",
    "carry_spread_bps": "套息利差(US2Y-JP2Y)",
    "curve_slope_bps": "收益率曲线斜率(10Y-2Y)",
    "on_rrp": "逆回购余额",
}

# Direction: +1 = higher is bullish, -1 = higher is bearish
IND_DIRECTION = {
    "net_liquidity": +1, "vix": -1, "hy_oas": -1, "sofr": -1,
    "dxy": -1, "carry_spread_bps": +1, "curve_slope_bps": +1, "on_rrp": -1,
}


class ForwardAnalyzer:
    """
    Generates forward-looking macro analysis by combining:
    - Quantitative trend/momentum signals
    - Historical analogue matching
    - AI narrative synthesis
    """

    def __init__(self, config: dict):
        self.config = config

    def analyze(self, panel: pd.DataFrame, signal_panel: pd.DataFrame,
                score_data: dict) -> dict:
        """
        Main entry point. Returns forward_analysis dict for latest.json.
        """
        logger.info("Forward Analysis: Starting...")

        result = {}

        # 1. Trend analysis per indicator
        trend_summary = self._compute_trends(panel, score_data)
        result['trend_summary'] = trend_summary
        logger.info(f"  Trends: {sum(1 for t in trend_summary.values() if t['momentum_signal'] == 'accelerating')} accelerating, "
                     f"{sum(1 for t in trend_summary.values() if t['momentum_signal'] == 'decelerating')} decelerating")

        # 2. Historical analogue matching
        analogues = self._find_analogues(panel, score_data)
        result['historical_analogues'] = analogues
        forward_stats = self._compute_forward_returns(analogues)
        result['forward_return_stats'] = forward_stats
        logger.info(f"  Found {len(analogues)} historical analogues")

        # 3. Regime transition probability
        regime_probs = self._regime_transition_probs(panel, score_data)
        result['regime_outlook'] = regime_probs

        # 4. Composite forward signal (quantitative)
        fwd_signal = self._composite_forward_signal(trend_summary, forward_stats, score_data)
        result['forward_signal'] = fwd_signal
        logger.info(f"  Forward signal: {fwd_signal['bias_cn']} (score: {fwd_signal['score']:.0f})")

        # 5. AI narrative (will be called separately with API key)
        result['ai_narrative'] = None  # Filled by generate_narrative()

        logger.info("Forward Analysis: Complete")
        return result

    def generate_narrative(self, forward_data: dict, score_data: dict, api_key: str) -> str:
        """
        Use LLM to synthesize a forward-looking narrative in Chinese.
        Called separately because it needs API key.
        """
        if not api_key:
            return None

        import json
        import requests as req

        trend = forward_data.get('trend_summary', {})
        fwd_stats = forward_data.get('forward_return_stats', {})
        fwd_signal = forward_data.get('forward_signal', {})
        regime = forward_data.get('regime_outlook', {})
        analogues = forward_data.get('historical_analogues', [])

        # Build context for AI
        trend_text = []
        for ind, t in trend.items():
            name = IND_NAME_CN.get(ind, ind)
            trend_text.append(
                f"- {name}: {t['direction_cn']}，5日变动{t['chg_5d_pct']:.1f}%，"
                f"20日变动{t['chg_20d_pct']:.1f}%，"
                f"动量{t['momentum_signal_cn']}（{'边际改善' if t['is_improving'] else '边际恶化'}）"
            )

        analogue_text = []
        for a in analogues[:5]:
            analogue_text.append(
                f"- {a['date']}: 当时评分{a['score_then']:.0f}，"
                f"之后20日SPX {a['spx_fwd_20d']:+.1f}%，BTC {a['btc_fwd_20d']:+.1f}%"
            )

        prompt = f"""你是一位顶级宏观策略分析师。基于以下量化数据，撰写一段**前瞻性分析**（300-500字中文）。

## 当前评分
综合评分: {score_data.get('composite_score', 'N/A')}/100 ({score_data.get('tier_cn', '')})

## 各指标趋势（近期方向+动量）
{chr(10).join(trend_text)}

## 历史类比（过去相似评分环境下的市场表现）
{chr(10).join(analogue_text) if analogue_text else '数据不足'}

## 量化前瞻信号
- 综合偏向: {fwd_signal.get('bias_cn', 'N/A')}（前瞻评分 {fwd_signal.get('score', 'N/A'):.0f}/100）
- 预期SPX 20日回报中位数: {fwd_stats.get('spx_median_20d', 'N/A')}%
- 预期BTC 20日回报中位数: {fwd_stats.get('btc_median_20d', 'N/A')}%
- 趋势改善率: {fwd_signal.get('improving_ratio', 0)*100:.0f}%的指标在边际改善

## 体制转换概率
- 当前: {regime.get('current_regime', 'N/A')}
- 维持概率: {regime.get('stay_prob', 0)*100:.0f}%
- 恶化概率: {regime.get('worsen_prob', 0)*100:.0f}%
- 改善概率: {regime.get('improve_prob', 0)*100:.0f}%

要求：
1. 第一段：当前宏观格局判断（不重复数字，用人话概括）
2. 第二段：关键变量的趋势方向分析（哪些在改善/恶化，边际最重要的2-3个指标）
3. 第三段：历史类比带来的前瞻判断（过去类似环境下市场怎么走的，典型周期多久）
4. 第四段：未来1-4周最可能的情景 + 需要关注的拐点信号
5. 最后一句：用一句话给出明确的多空倾向和建议仓位区间

风格：像华尔街首席策略师的周报，语言简洁有力，有观点有逻辑，不废话。"""

        base_url = self.config.get('daily_brief', {}).get('analysis', {}).get(
            'base_url', 'https://openrouter.ai/api/v1')
        model = self.config.get('daily_brief', {}).get('analysis', {}).get(
            'model', 'google/gemini-2.0-flash-001')

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://invest-wine.vercel.app",
            "X-Title": "Macro Forward Analysis",
        }

        try:
            resp = req.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000,
                },
                timeout=90,
            )
            resp.raise_for_status()
            narrative = resp.json()["choices"][0]["message"]["content"]
            logger.info("Forward narrative generated via AI")
            return narrative
        except Exception as e:
            logger.warning(f"Forward narrative AI failed: {e}")
            return None

    # ==============================================================
    # 1. TREND ANALYSIS
    # ==============================================================
    def _compute_trends(self, panel: pd.DataFrame, score_data: dict) -> dict:
        """
        For each indicator, compute:
        - Direction (rising/falling)
        - Velocity (rate of change)
        - Acceleration (is it getting faster or slower?)
        - Whether it's improving or worsening for risk assets
        """
        trends = {}
        for ind in IND_DIRECTION:
            if ind not in panel.columns:
                continue
            series = panel[ind].dropna()
            if len(series) < 25:
                continue

            direction = IND_DIRECTION[ind]
            current = series.iloc[-1]

            # Changes
            chg_5d = series.iloc[-1] - series.iloc[-6] if len(series) > 5 else 0
            chg_10d = series.iloc[-1] - series.iloc[-11] if len(series) > 10 else 0
            chg_20d = series.iloc[-1] - series.iloc[-21] if len(series) > 20 else 0

            # Percentage changes (use abs of base for normalisation)
            base = abs(series.iloc[-6]) if len(series) > 5 and series.iloc[-6] != 0 else 1
            chg_5d_pct = (chg_5d / base) * 100
            base20 = abs(series.iloc[-21]) if len(series) > 20 and series.iloc[-21] != 0 else 1
            chg_20d_pct = (chg_20d / base20) * 100

            # Velocity: 5d rate of change
            velocity = chg_5d_pct

            # Acceleration: compare recent 5d change vs prior 5d change
            if len(series) > 10:
                prev_chg_5d = series.iloc[-6] - series.iloc[-11]
                prev_base = abs(series.iloc[-11]) if series.iloc[-11] != 0 else 1
                prev_velocity = (prev_chg_5d / prev_base) * 100
                acceleration = velocity - prev_velocity
            else:
                prev_velocity = 0
                acceleration = 0

            # Direction classification
            if abs(chg_5d_pct) < 0.1:
                dir_label = "sideways"
                dir_cn = "横盘"
            elif chg_5d_pct > 0:
                dir_label = "rising"
                dir_cn = "上行"
            else:
                dir_label = "falling"
                dir_cn = "下行"

            # Momentum: accelerating or decelerating?
            if abs(acceleration) < 0.05:
                momentum = "steady"
                momentum_cn = "稳定"
            elif (velocity > 0 and acceleration > 0) or (velocity < 0 and acceleration < 0):
                momentum = "accelerating"
                momentum_cn = "加速"
            else:
                momentum = "decelerating"
                momentum_cn = "减速"

            # Is this improving for risk assets?
            # direction * change > 0 means indicator moving in bullish direction
            bullish_move = chg_5d * direction
            is_improving = bullish_move > 0

            trends[ind] = {
                "current": round(float(current), 4),
                "chg_5d": round(float(chg_5d), 4),
                "chg_10d": round(float(chg_10d), 4),
                "chg_20d": round(float(chg_20d), 4),
                "chg_5d_pct": round(float(chg_5d_pct), 2),
                "chg_20d_pct": round(float(chg_20d_pct), 2),
                "velocity": round(float(velocity), 2),
                "acceleration": round(float(acceleration), 2),
                "direction": dir_label,
                "direction_cn": dir_cn,
                "momentum_signal": momentum,
                "momentum_signal_cn": momentum_cn,
                "is_improving": bool(is_improving),
                "is_improving_cn": "边际改善" if is_improving else "边际恶化",
            }

        return trends

    # ==============================================================
    # 2. HISTORICAL ANALOGUE MATCHING
    # ==============================================================
    def _find_analogues(self, panel: pd.DataFrame, score_data: dict,
                        top_n: int = 10, min_distance_days: int = 20) -> list:
        """
        Find past periods with similar indicator profiles.
        Uses cosine similarity on z-score normalized indicator values.
        Returns top_n analogues with forward returns.
        """
        indicators = [k for k in IND_DIRECTION if k in panel.columns]
        if not indicators or len(panel) < 60:
            return []

        # Current z-scores (60d rolling)
        current_profile = {}
        for ind in indicators:
            series = panel[ind].dropna()
            if len(series) < 60:
                continue
            mean = series.iloc[-60:].mean()
            std = series.iloc[-60:].std()
            if std > 0:
                current_profile[ind] = (series.iloc[-1] - mean) / std
            else:
                current_profile[ind] = 0

        if len(current_profile) < 4:
            return []

        # Build normalized matrix for all history
        common_inds = list(current_profile.keys())
        current_vec = np.array([current_profile[k] for k in common_inds])

        analogues = []
        # Slide through history (skip first 60 days and last 25 days)
        for i in range(60, len(panel) - 25):
            row = panel.iloc[i]
            # Compute z-scores at this point using trailing 60d
            profile = []
            valid = True
            for ind in common_inds:
                hist = panel[ind].iloc[max(0, i-60):i+1].dropna()
                if len(hist) < 30:
                    valid = False
                    break
                m = hist.mean()
                s = hist.std()
                if s > 0:
                    profile.append((row[ind] - m) / s if not pd.isna(row.get(ind)) else 0)
                else:
                    profile.append(0)
            if not valid:
                continue

            hist_vec = np.array(profile)

            # Cosine similarity
            dot = np.dot(current_vec, hist_vec)
            norm = np.linalg.norm(current_vec) * np.linalg.norm(hist_vec)
            if norm == 0:
                continue
            similarity = dot / norm

            # Only keep high similarity
            if similarity < 0.7:
                continue

            # Compute forward returns
            date_str = str(panel.index[i].date()) if hasattr(panel.index[i], 'date') else str(panel.index[i])

            spx_fwd_5d = spx_fwd_10d = spx_fwd_20d = None
            btc_fwd_5d = btc_fwd_10d = btc_fwd_20d = None

            if 'spx' in panel.columns:
                spx = panel['spx']
                if not pd.isna(spx.iloc[i]) and spx.iloc[i] > 0:
                    if i + 5 < len(panel) and not pd.isna(spx.iloc[i+5]):
                        spx_fwd_5d = round((spx.iloc[i+5] / spx.iloc[i] - 1) * 100, 2)
                    if i + 10 < len(panel) and not pd.isna(spx.iloc[i+10]):
                        spx_fwd_10d = round((spx.iloc[i+10] / spx.iloc[i] - 1) * 100, 2)
                    if i + 20 < len(panel) and not pd.isna(spx.iloc[i+20]):
                        spx_fwd_20d = round((spx.iloc[i+20] / spx.iloc[i] - 1) * 100, 2)

            if 'btc' in panel.columns:
                btc = panel['btc']
                if not pd.isna(btc.iloc[i]) and btc.iloc[i] > 0:
                    if i + 5 < len(panel) and not pd.isna(btc.iloc[i+5]):
                        btc_fwd_5d = round((btc.iloc[i+5] / btc.iloc[i] - 1) * 100, 2)
                    if i + 10 < len(panel) and not pd.isna(btc.iloc[i+10]):
                        btc_fwd_10d = round((btc.iloc[i+10] / btc.iloc[i] - 1) * 100, 2)
                    if i + 20 < len(panel) and not pd.isna(btc.iloc[i+20]):
                        btc_fwd_20d = round((btc.iloc[i+20] / btc.iloc[i] - 1) * 100, 2)

            analogues.append({
                "date": date_str,
                "similarity": round(float(similarity), 3),
                "score_then": round(float(np.mean([abs(z) for z in profile]) * 50), 1),  # Rough score proxy
                "spx_fwd_5d": spx_fwd_5d,
                "spx_fwd_10d": spx_fwd_10d,
                "spx_fwd_20d": spx_fwd_20d,
                "btc_fwd_5d": btc_fwd_5d,
                "btc_fwd_10d": btc_fwd_10d,
                "btc_fwd_20d": btc_fwd_20d,
            })

        # Sort by similarity, then deduplicate (min 20 trading days apart)
        analogues.sort(key=lambda x: -x['similarity'])
        filtered = []
        used_dates = set()
        for a in analogues:
            # Simple date-distance filter
            date_key = a['date'][:7]  # month-level dedup
            if date_key not in used_dates:
                filtered.append(a)
                used_dates.add(date_key)
            if len(filtered) >= top_n:
                break

        return filtered

    def _compute_forward_returns(self, analogues: list) -> dict:
        """
        Compute aggregate forward return statistics from analogues.
        """
        if not analogues:
            return {
                'spx_median_5d': None, 'spx_median_10d': None, 'spx_median_20d': None,
                'btc_median_5d': None, 'btc_median_10d': None, 'btc_median_20d': None,
                'n_analogues': 0, 'win_rate_spx_20d': None, 'win_rate_btc_20d': None,
            }

        def stats(key):
            vals = [a[key] for a in analogues if a.get(key) is not None]
            if not vals:
                return None, None, None, None
            arr = np.array(vals)
            return (round(float(np.median(arr)), 2),
                    round(float(np.percentile(arr, 25)), 2),
                    round(float(np.percentile(arr, 75)), 2),
                    round(float(np.mean(arr > 0)), 2))

        spx5 = stats('spx_fwd_5d')
        spx10 = stats('spx_fwd_10d')
        spx20 = stats('spx_fwd_20d')
        btc5 = stats('btc_fwd_5d')
        btc10 = stats('btc_fwd_10d')
        btc20 = stats('btc_fwd_20d')

        return {
            'n_analogues': len(analogues),
            'spx_median_5d': spx5[0], 'spx_p25_5d': spx5[1], 'spx_p75_5d': spx5[2],
            'spx_median_10d': spx10[0], 'spx_p25_10d': spx10[1], 'spx_p75_10d': spx10[2],
            'spx_median_20d': spx20[0], 'spx_p25_20d': spx20[1], 'spx_p75_20d': spx20[2],
            'win_rate_spx_20d': spx20[3],
            'btc_median_5d': btc5[0], 'btc_p25_5d': btc5[1], 'btc_p75_5d': btc5[2],
            'btc_median_10d': btc10[0], 'btc_p25_10d': btc10[1], 'btc_p75_10d': btc10[2],
            'btc_median_20d': btc20[0], 'btc_p25_20d': btc20[1], 'btc_p75_20d': btc20[2],
            'win_rate_btc_20d': btc20[3],
        }

    # ==============================================================
    # 3. REGIME TRANSITION PROBABILITY
    # ==============================================================
    def _regime_transition_probs(self, panel: pd.DataFrame, score_data: dict) -> dict:
        """
        Estimate probability of regime transition based on historical patterns.
        Simple approach: bucket composite scores, compute transition matrix.
        """
        # We need to compute rolling composite scores for history
        # Simplified: use net_liquidity percentile as a proxy
        if 'net_liquidity' not in panel.columns or len(panel) < 100:
            return {
                'current_regime': score_data.get('tier_cn', '未知'),
                'stay_prob': 0.6, 'improve_prob': 0.2, 'worsen_prob': 0.2,
            }

        # Bucket the composite score based on percentile of key indicators
        nl = panel['net_liquidity'].dropna()
        scores_hist = []
        for i in range(60, len(nl)):
            pctile = (nl.iloc[:i] < nl.iloc[i]).sum() / i * 100
            scores_hist.append(pctile)

        if len(scores_hist) < 50:
            return {
                'current_regime': score_data.get('tier_cn', '未知'),
                'stay_prob': 0.6, 'improve_prob': 0.2, 'worsen_prob': 0.2,
            }

        # Classify into regimes: bear (<35), neutral (35-65), bull (>65)
        regimes = []
        for s in scores_hist:
            if s < 35:
                regimes.append('bear')
            elif s < 65:
                regimes.append('neutral')
            else:
                regimes.append('bull')

        # Count transitions
        transitions = {}
        for i in range(len(regimes) - 5):  # 5-day forward transition
            current = regimes[i]
            future = regimes[i + 5]
            if current not in transitions:
                transitions[current] = {'bear': 0, 'neutral': 0, 'bull': 0}
            transitions[current][future] += 1

        # Current regime
        current_score = score_data.get('composite_score', 50)
        if current_score < 35:
            current_regime = 'bear'
        elif current_score < 65:
            current_regime = 'neutral'
        else:
            current_regime = 'bull'

        # Get probabilities
        if current_regime in transitions:
            total = sum(transitions[current_regime].values())
            if total > 0:
                probs = {k: v / total for k, v in transitions[current_regime].items()}
            else:
                probs = {'bear': 0.33, 'neutral': 0.34, 'bull': 0.33}
        else:
            probs = {'bear': 0.33, 'neutral': 0.34, 'bull': 0.33}

        # Classify into stay/improve/worsen
        regime_order = ['bear', 'neutral', 'bull']
        idx = regime_order.index(current_regime)
        stay_prob = probs.get(current_regime, 0.33)
        improve_prob = sum(probs.get(r, 0) for r in regime_order[idx+1:]) if idx < 2 else 0
        worsen_prob = sum(probs.get(r, 0) for r in regime_order[:idx]) if idx > 0 else 0

        return {
            'current_regime': score_data.get('tier_cn', '未知'),
            'current_regime_key': current_regime,
            'stay_prob': round(float(stay_prob), 3),
            'improve_prob': round(float(improve_prob), 3),
            'worsen_prob': round(float(worsen_prob), 3),
            'transition_matrix': {k: {kk: round(vv / max(sum(v.values()), 1), 3) for kk, vv in v.items()}
                                  for k, v in transitions.items()},
        }

    # ==============================================================
    # 4. COMPOSITE FORWARD SIGNAL
    # ==============================================================
    def _composite_forward_signal(self, trends: dict, fwd_stats: dict,
                                   score_data: dict) -> dict:
        """
        Combine all quantitative signals into a single forward-looking score.
        """
        # Component 1: Current score (40%)
        current_score = score_data.get('composite_score', 50)

        # Component 2: Trend momentum (30%)
        # How many indicators are improving?
        improving = sum(1 for t in trends.values() if t['is_improving'])
        total = max(len(trends), 1)
        improving_ratio = improving / total
        momentum_score = improving_ratio * 100

        # Component 3: Historical precedent (30%)
        # Based on median forward return of analogues
        spx_med = fwd_stats.get('spx_median_20d')
        btc_med = fwd_stats.get('btc_median_20d')

        if spx_med is not None and btc_med is not None:
            # Normalize: +5% → 100, -5% → 0, 0% → 50
            hist_score = 50 + (spx_med * 5 + btc_med * 3) / 2  # Weighted blend
            hist_score = max(0, min(100, hist_score))
        elif spx_med is not None:
            hist_score = 50 + spx_med * 8
            hist_score = max(0, min(100, hist_score))
        else:
            hist_score = 50  # No data, neutral

        # Composite
        forward_score = current_score * 0.40 + momentum_score * 0.30 + hist_score * 0.30
        forward_score = max(0, min(100, forward_score))

        # Bias label
        if forward_score >= 70:
            bias = "bullish"
            bias_cn = "偏多"
        elif forward_score >= 55:
            bias = "mild_bull"
            bias_cn = "略偏多"
        elif forward_score >= 45:
            bias = "neutral"
            bias_cn = "中性"
        elif forward_score >= 30:
            bias = "mild_bear"
            bias_cn = "略偏空"
        else:
            bias = "bearish"
            bias_cn = "偏空"

        return {
            'score': round(float(forward_score), 1),
            'bias': bias,
            'bias_cn': bias_cn,
            'components': {
                'current_score': round(float(current_score), 1),
                'momentum_score': round(float(momentum_score), 1),
                'historical_score': round(float(hist_score), 1),
            },
            'improving_ratio': round(float(improving_ratio), 2),
            'improving_count': improving,
            'total_indicators': total,
        }
