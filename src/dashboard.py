"""
dashboard.py - HTML dashboard generator (v2)
Single-page layout with embedded charts and contextual analysis.
Each indicator section includes: what it is, what high/low means,
current reading interpretation, and investment implications.
"""

import json
import base64
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================
# Indicator Knowledge Base
# Each entry: what, high_means, low_means, investment_note
# These are static educational context; dynamic interpretation
# is generated from live data in _interpret().
# ============================================================
INDICATOR_KNOWLEDGE = {
    "net_liquidity": {
        "title": "净流动性 (Net Liquidity)",
        "formula": "美联储总资产 - 财政部TGA账户 - 隔夜逆回购(ON RRP)",
        "what": "衡量美联储实际向金融体系释放了多少可用流动性。这是整个流动性框架的核心锚定指标。当美联储扩表但TGA或ON RRP也同步上升时，实际流入市场的资金可能并没有增加。",
        "high_means": "金融体系水位充裕，银行间资金宽松，风险资产倾向获得支撑。历史上净流动性上行周期与美股上涨高度相关。",
        "low_means": "系统性流动性收紧，银行准备金下降，资金成本上升压力增大。可能触发风险资产回调，尤其当降速过快时。",
        "chart_key": "net_liquidity",
    },
    "composite": {
        "title": "净流动性 vs S&P 500 (风险确认)",
        "what": "将净流动性与标普500叠加对比，验证'流动性驱动风险偏好'的核心逻辑。两者长期正相关。当出现背离（流动性下行但股市仍涨），往往意味着市场在消耗存量动能，需要警惕补跌风险。",
        "high_means": "两者同步上行 = 流动性驱动的健康牛市。",
        "low_means": "两者背离 = 流动性基础松动，风险资产可能滞后反应。",
        "chart_key": "composite_netliq_spx",
    },
    "sofr": {
        "title": "SOFR (担保隔夜融资利率)",
        "what": "美国短期资金市场的基准利率，反映银行间担保借贷的实际成本。SOFR紧贴美联储联邦基金利率区间运行，如果显著偏离（尤其向上），说明短期融资市场出现紧张。",
        "high_means": "短端融资成本上升，可能反映准备金不足或回购市场紧张。2019年9月回购危机时SOFR曾飙升至5.25%以上。持续走高对杠杆策略不利。",
        "low_means": "资金面宽松，借贷成本低，有利于杠杆策略和短久期套利。",
        "chart_key": "sofr",
    },
    "move_proxy": {
        "title": "利率波动代理 (MOVE Proxy)",
        "what": "原始MOVE指数(ICE BofA)为付费数据，此处用VIX × 国债收益率波动率构建代理。反映债券市场的隐含波动预期。利率波动率升高通常先于股市波动，是重要的前瞻信号。",
        "high_means": "债市恐慌加剧，固收交易员在对冲尾部风险。通常伴随国债大幅抛售或政策不确定性。利率波动升高→杠杆基金被迫去杠杆→可能传导至风险资产。",
        "low_means": "利率市场平静，风险偏好稳定，有利于carry和duration策略。",
        "chart_key": "move_proxy",
    },
    "hy_oas": {
        "title": "高收益信用利差 (HY OAS)",
        "what": "ICE BofA美国高收益债相对国债的期权调整价差。这是信用市场的'恐慌指标'——当投资者对企业违约风险担忧加剧时，会要求更高的信用补偿。HY OAS是检验流动性收紧是否已传导至信用层的关键确认指标。",
        "high_means": "信用市场定价违约风险上升。超过500bps进入'压力区'，超过800bps进入'危机区'。信用利差走阔通常领先于经济衰退3-6个月。对高收益债、杠杆贷款、信用敏感股票极其不利。",
        "low_means": "信用市场乐观，企业融资环境友好。低于300bps可能暗示过度乐观，需关注是否在定价充分的风险溢价。",
        "chart_key": "hy_oas",
    },
    "usdjpy": {
        "title": "USD/JPY (美元兑日元)",
        "what": "全球套息交易(Carry Trade)最重要的风向标。日元是全球主要的融资货币——投资者借入低息日元、投资高息美元资产。USD/JPY的走势直接反映套息交易的拥挤程度和平仓风险。",
        "high_means": "日元走弱、套息交易盈利扩大、全球Risk-On。但极端高位（如超过155-160）可能触发日本央行干预风险，一旦干预→套息快速平仓→全球风险资产闪崩（参考2024年8月5日事件）。",
        "low_means": "日元走强、套息交易平仓、全球去杠杆压力上升。USD/JPY快速下跌是最危险的宏观信号之一，意味着全球流动性链条可能断裂。",
        "chart_key": "usdjpy",
    },
    "carry_spread": {
        "title": "套息利差 (US 2Y - JP 2Y)",
        "what": "美国2年期国债收益率减去日本2年期国债收益率，衡量套息交易的利差基础。利差越大，套息交易的'票息收入'越丰厚，吸引更多资金做多美元做空日元。利差收窄则削弱套息动力。",
        "high_means": "套息交易有强利差支撑，全球资金倾向流入美元资产。有利于美股、美元，不利于新兴市场。",
        "low_means": "套息动力衰减。如果因为美国降息或日本加息导致利差快速收窄，会触发大规模套息平仓，冲击全球风险资产。",
        "chart_key": "carry_spread_bps",
    },
    "curve_slope": {
        "title": "收益率曲线斜率 (10Y - 2Y)",
        "what": "10年期与2年期美债利差，反映市场对经济前景和货币政策路径的定价。这是最经典的经济周期前瞻指标之一。",
        "high_means": "曲线陡峭化：市场预期经济向好或通胀上行，长端利率走高。银行盈利改善（借短贷长），但可能意味着通胀预期失锚。",
        "low_means": "曲线平坦化或倒挂：市场预期经济放缓或衰退，压低长端利率。持续倒挂是衰退的最可靠先行指标（历史准确率极高，领先约12-18个月）。",
        "chart_key": "curve_slope_bps",
    },
    "vix": {
        "title": "VIX (恐慌指数)",
        "what": "标普500指数期权的30天隐含波动率，反映市场对未来一个月股市波动的预期。VIX是全球最广泛使用的市场情绪温度计。",
        "high_means": "市场恐慌加剧。20-25区间为'警戒'，25-30为'紧张'，30以上为'恐慌'。VIX飙升通常伴随股市急跌和流动性收紧。但极端高位（>35）反而可能是底部信号（恐慌达峰→反转）。",
        "low_means": "市场极度平静。低于15为'自满区'。持续低波动可能孕育尾部风险——波动率均值回归的力量很强，长时间低波之后往往出现剧烈波动（'明斯基时刻'）。",
        "chart_key": "vix",
    },
    "spx": {
        "title": "S&P 500 (标普500)",
        "what": "美国大盘股基准指数，全球风险资产的锚。在流动性分析框架中，SPX的作用是'确认指标'——流动性前置信号出现后，观察SPX是否跟随反应，以判断信号的有效性。",
        "high_means": "风险偏好强劲。如果同时净流动性充裕，属于'流动性驱动牛市'，趋势可持续；如果净流动性已走弱但SPX仍创新高，需要警惕'背离'风险。",
        "low_means": "风险偏好恶化。如果与流动性收紧同步下跌，确认'趋紧'判断；如果流动性仍宽松但SPX下跌，可能是其他因素驱动（如盈利恶化、地缘风险），需区分对待。",
        "chart_key": "spx",
    },
    "dxy": {
        "title": "美元指数 (DXY)",
        "what": "美元对一篮子主要货币（欧元为主权重）的加权指数。美元是全球流动性的'反向指标'——美元走强通常意味着全球美元流动性收紧，对新兴市场和大宗商品形成压力。",
        "high_means": "全球美元流动性紧缩，资金回流美国。不利于非美资产、大宗商品、新兴市场。超过110为'强美元'区间，会加剧全球债务压力。",
        "low_means": "美元流动性外溢，全球Risk-On。有利于非美资产、大宗商品、新兴市场股债。",
        "chart_key": "dxy",
    },
    "btc": {
        "title": "Bitcoin (BTC/USD)",
        "what": "加密资产代表，在宏观流动性框架中作为'高Beta流动性敏感资产'。BTC对全球流动性变化极度敏感——几乎是净流动性的杠杆版本。它的走势可以验证流动性信号的强度。",
        "high_means": "流动性极度充裕，投机情绪高涨。BTC创新高通常伴随全球流动性周期顶部。",
        "low_means": "流动性收紧的'矿井金丝雀'——BTC往往比传统风险资产更早、更猛烈地反映流动性收缩。如果BTC大幅下跌但SPX尚未反应，可能是前瞻预警。",
        "chart_key": "btc",
    },
}


class DashboardGenerator:
    """Generate a self-contained single-page HTML dashboard."""

    def __init__(self, config: dict):
        self.output_dir = Path(config.get("output", {}).get("base_dir", "output"))
        self.chart_dir = self.output_dir / "charts"

    def generate(self, summary: dict, score_data: dict = None) -> str:
        charts_b64 = self._encode_charts()
        html = self._build_html(summary, charts_b64, score_data)
        output_path = self.output_dir / "dashboard.html"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"Dashboard saved: {output_path} ({len(html)//1024}KB)")
        return str(output_path)

    def _encode_charts(self) -> dict:
        b64 = {}
        if not self.chart_dir.exists():
            return b64
        for f in sorted(self.chart_dir.iterdir()):
            if f.suffix == ".png":
                with open(f, "rb") as fh:
                    b64[f.stem] = base64.b64encode(fh.read()).decode()
        logger.info(f"Encoded {len(b64)} charts for dashboard")
        return b64

    def _signal_badge(self, sig: str) -> str:
        colors = {
            "STRESS": ("🔴", "#fee2e2", "#dc2626"),
            "TIGHT": ("🟡", "#fef3c7", "#d97706"),
            "EASING": ("🟢", "#d1fae5", "#059669"),
            "NEUTRAL": ("⚪", "#f3f4f6", "#6b7280"),
        }
        icon, bg, color = colors.get(str(sig), colors["NEUTRAL"])
        return (
            f'<span style="background:{bg};color:{color};padding:2px 8px;'
            f'border-radius:10px;font-size:12px;font-weight:600">{icon} {sig}</span>'
        )

    def _interpret(self, key: str, readings: dict, changes: dict) -> str:
        """Generate dynamic interpretation text based on current data."""
        r = readings.get(key, {})
        val = r.get("value")
        zscore = r.get("zscore")
        pctl = r.get("percentile")
        sig = r.get("signal", "NEUTRAL")

        if val is None:
            return '<span style="color:#94a3b8">数据暂不可用。</span>'

        parts = []

        # Level context
        if pctl is not None:
            pctl_pct = pctl * 100
            if pctl_pct > 80:
                parts.append(f"当前值处于近1年 <strong>{pctl_pct:.0f}%</strong> 分位（偏高区间）")
            elif pctl_pct < 20:
                parts.append(f"当前值处于近1年 <strong>{pctl_pct:.0f}%</strong> 分位（偏低区间）")
            else:
                parts.append(f"当前值处于近1年 <strong>{pctl_pct:.0f}%</strong> 分位（中性区间）")

        # Z-score context
        if zscore is not None:
            if abs(zscore) > 1.5:
                parts.append(f"Z-Score <strong>{zscore:+.2f}</strong>（显著偏离均值）")
            elif abs(zscore) > 0.5:
                parts.append(f"Z-Score <strong>{zscore:+.2f}</strong>（轻度偏离）")
            else:
                parts.append(f"Z-Score <strong>{zscore:+.2f}</strong>（接近均值）")

        # Change context
        c = changes.get(key, {})
        chg_5d = c.get("pct_5d") if key in ("spx", "btc", "usdjpy", "dxy") else c.get("chg_5d")
        chg_20d = c.get("pct_20d") if key in ("spx", "btc", "usdjpy", "dxy") else c.get("chg_20d")

        if chg_5d is not None and chg_20d is not None:
            bad_up = key in ("sofr", "hy_oas", "vix", "move_proxy", "dxy")
            if key in ("spx", "btc", "usdjpy", "dxy"):
                chg5_str = f"{chg_5d*100:+.1f}%"
                chg20_str = f"{chg_20d*100:+.1f}%"
            else:
                chg5_str = f"{chg_5d:+.2f}"
                chg20_str = f"{chg_20d:+.2f}"

            direction = ""
            if bad_up:
                if chg_5d > 0 and chg_20d > 0:
                    direction = "持续恶化（5日/20日均上行）"
                elif chg_5d < 0 and chg_20d < 0:
                    direction = "持续改善（5日/20日均下行）"
                elif chg_5d > 0 and chg_20d <= 0:
                    direction = "短期反弹恶化，中期趋势尚可"
                else:
                    direction = "短期改善，但中期仍偏紧"
            else:
                bad_down = key in ("net_liquidity", "usdjpy", "carry_spread_bps", "spx", "btc")
                if bad_down:
                    if chg_5d > 0 and chg_20d > 0:
                        direction = "持续改善（5日/20日均上行）"
                    elif chg_5d < 0 and chg_20d < 0:
                        direction = "持续恶化（5日/20日均下行）"
                    elif chg_5d > 0 and chg_20d <= 0:
                        direction = "短期反弹，但中期趋势仍偏弱"
                    else:
                        direction = "短期回落，中期趋势尚可"
                else:
                    direction = f"5日变动 {chg5_str}，20日变动 {chg20_str}"

            parts.append(f"近期走势：{direction}（5日 {chg5_str} / 20日 {chg20_str}）")

        # Signal interpretation
        signal_text = {
            "STRESS": "🔴 当前发出<strong>压力信号</strong>，需要密切关注。",
            "TIGHT": "🟡 当前偏紧，尚未进入压力区，但需保持警惕。",
            "EASING": "🟢 当前偏宽松，环境有利。",
            "NEUTRAL": "⚪ 当前中性，无明显方向性信号。",
        }
        parts.append(signal_text.get(sig, ""))

        return "。".join(p for p in parts if p) + "" if parts else ""

    def _build_score_section(self, score_data: dict) -> str:
        """Build the composite score + investment advice section."""
        if not score_data:
            return ""

        cs = score_data["composite_score"]
        tier_cn = score_data["tier_cn"]
        tier_color = score_data["tier_color"]
        tier_emoji = score_data["tier_emoji"]
        advice = score_data.get("investment_advice", {})
        outlook = score_data.get("risk_asset_outlook", {})
        weights = score_data.get("weight_table", {})
        ind_scores = score_data.get("individual_scores", {})

        # Gauge arc (SVG) - score from 0 to 100 mapped to arc
        # Arc goes from -135deg to +135deg (270 deg total)
        angle = -135 + (cs / 100) * 270
        rad = angle * 3.14159 / 180
        import math
        cx, cy, r = 120, 120, 90
        # Calculate arc endpoint
        end_x = cx + r * math.cos(rad)
        end_y = cy + r * math.sin(rad)

        # Needle
        needle_len = 75
        nx = cx + needle_len * math.cos(rad)
        ny = cy + needle_len * math.sin(rad)

        # Color zones for the arc
        gauge_svg = f'''
        <svg viewBox="0 0 240 160" style="width:240px;height:160px">
            <!-- Background arc -->
            <path d="M {cx + r*math.cos(-135*3.14159/180)} {cy + r*math.sin(-135*3.14159/180)}
                     A {r} {r} 0 0 1 {cx + r*math.cos(-90*3.14159/180)} {cy + r*math.sin(-90*3.14159/180)}"
                  fill="none" stroke="#991b1b" stroke-width="16" stroke-linecap="round" opacity="0.3"/>
            <path d="M {cx + r*math.cos(-90*3.14159/180)} {cy + r*math.sin(-90*3.14159/180)}
                     A {r} {r} 0 0 1 {cx + r*math.cos(-45*3.14159/180)} {cy + r*math.sin(-45*3.14159/180)}"
                  fill="none" stroke="#ef4444" stroke-width="16" stroke-linecap="butt" opacity="0.3"/>
            <path d="M {cx + r*math.cos(-45*3.14159/180)} {cy + r*math.sin(-45*3.14159/180)}
                     A {r} {r} 0 0 1 {cx + r*math.cos(0*3.14159/180)} {cy + r*math.sin(0*3.14159/180)}"
                  fill="none" stroke="#eab308" stroke-width="16" stroke-linecap="butt" opacity="0.3"/>
            <path d="M {cx + r*math.cos(0*3.14159/180)} {cy + r*math.sin(0*3.14159/180)}
                     A {r} {r} 0 0 1 {cx + r*math.cos(45*3.14159/180)} {cy + r*math.sin(45*3.14159/180)}"
                  fill="none" stroke="#22c55e" stroke-width="16" stroke-linecap="butt" opacity="0.3"/>
            <path d="M {cx + r*math.cos(45*3.14159/180)} {cy + r*math.sin(45*3.14159/180)}
                     A {r} {r} 0 0 1 {cx + r*math.cos(135*3.14159/180)} {cy + r*math.sin(135*3.14159/180)}"
                  fill="none" stroke="#16a34a" stroke-width="16" stroke-linecap="round" opacity="0.3"/>
            <!-- Needle -->
            <line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}"
                  stroke="{tier_color}" stroke-width="3" stroke-linecap="round"/>
            <circle cx="{cx}" cy="{cy}" r="6" fill="{tier_color}"/>
            <!-- Labels -->
            <text x="30" y="145" font-size="10" fill="#94a3b8">0</text>
            <text x="110" y="20" font-size="10" fill="#94a3b8" text-anchor="middle">50</text>
            <text x="205" y="145" font-size="10" fill="#94a3b8">100</text>
        </svg>
        '''

        # Individual indicator score bars
        ind_name_map = {
            "net_liquidity": "净流动性",
            "vix": "VIX恐慌指数",
            "hy_oas": "高收益信用利差",
            "sofr": "SOFR资金利率",
            "dxy": "美元指数",
            "carry_spread_bps": "套息利差",
            "curve_slope_bps": "收益率曲线",
            "on_rrp": "逆回购(RRP)",
        }

        bars_html = ""
        for name, info in sorted(ind_scores.items(), key=lambda x: -x[1]["score"]):
            s = info["score"]
            w = weights.get(name, 0)
            label = ind_name_map.get(name, name)
            bar_color = info["signal_color"]
            bars_html += f'''
            <div class="score-bar-row">
                <div class="score-bar-label">{label} <span class="score-bar-weight">({w}%)</span></div>
                <div class="score-bar-track">
                    <div class="score-bar-fill" style="width:{s}%;background:{bar_color}"></div>
                </div>
                <div class="score-bar-val" style="color:{bar_color}">{s:.0f}</div>
                <div class="score-bar-signal">{info["signal_cn"]}</div>
            </div>'''

        # Asset outlook cards
        asset_cards = ""
        asset_icons = {"btc": "₿", "spx": "📈", "nasdaq": "💻"}
        asset_labels = {"btc": "Bitcoin", "spx": "S&P 500", "nasdaq": "纳斯达克"}
        for asset_key in ["btc", "spx", "nasdaq"]:
            a = outlook.get(asset_key, {})
            if not a:
                continue
            asset_cards += f'''
            <div class="asset-card">
                <div class="asset-icon">{asset_icons.get(asset_key, "")}</div>
                <div class="asset-name">{asset_labels.get(asset_key, asset_key)}</div>
                <div class="asset-score" style="color:{a['color']}">{a['score']:.0f}</div>
                <div class="asset-tier" style="background:{a['color']}20;color:{a['color']};border:1px solid {a['color']}">{a['tier_cn']}</div>
                <div class="asset-note">{a['note']}</div>
            </div>'''

        # Bullish / bearish factors
        bull_items = ""
        bear_items = ""
        for name, score, sig_cn in advice.get("bullish_factors", []):
            lbl = ind_name_map.get(name, name)
            bull_items += f'<div class="factor-item bull">✅ {lbl}（{score:.0f}分 {sig_cn}）</div>'
        for name, score, sig_cn in advice.get("bearish_factors", []):
            lbl = ind_name_map.get(name, name)
            bear_items += f'<div class="factor-item bear">⚠️ {lbl}（{score:.0f}分 {sig_cn}）</div>'

        return f'''
<!-- ============ COMPOSITE SCORE ============ -->
<div class="score-hero">
    <div class="score-hero-left">
        <div class="score-hero-title">宏观流动性综合评分</div>
        <div class="score-hero-number" style="color:{tier_color}">{cs:.0f}</div>
        <div class="score-hero-tier" style="background:{tier_color}18;color:{tier_color};border:2px solid {tier_color}">
            {tier_emoji} {tier_cn}
        </div>
        <div class="score-hero-gauge">{gauge_svg}</div>
    </div>
    <div class="score-hero-right">
        <div class="advice-card">
            <div class="advice-position" style="color:{tier_color}">{advice.get("position","")}</div>
            <div class="advice-detail">{advice.get("position_detail","")}</div>
            <div class="advice-actions">
                <div class="advice-action">{advice.get("btc_action","")}</div>
                <div class="advice-action">{advice.get("spx_action","")}</div>
                <div class="advice-action">{advice.get("nasdaq_action","")}</div>
            </div>
            <div class="advice-risk">{advice.get("key_risk","")}</div>
            <div class="advice-catalyst">{advice.get("key_catalyst","")}</div>
        </div>
    </div>
</div>

<!-- Asset Outlook -->
<div class="asset-outlook">
    <div class="asset-outlook-title">风险资产前瞻</div>
    <div class="asset-grid">{asset_cards}</div>
</div>

<!-- Indicator Score Breakdown -->
<div class="score-breakdown">
    <div class="score-breakdown-title">各指标评分明细</div>
    <div class="score-breakdown-subtitle">评分0-100，越高越利多风险资产 | 权重基于5年回测相关性+宏观研究</div>
    <div class="score-bars">{bars_html}</div>
    <div class="score-factors">
        <div class="factors-col">
            <div class="factors-title" style="color:#16a34a">利多因素</div>
            {bull_items if bull_items else '<div class="factor-item" style="color:#94a3b8">暂无明显利多</div>'}
        </div>
        <div class="factors-col">
            <div class="factors-title" style="color:#dc2626">利空因素</div>
            {bear_items if bear_items else '<div class="factor-item" style="color:#94a3b8">暂无明显利空</div>'}
        </div>
    </div>
</div>

<div class="score-note">
    评分方法：每个指标综合「当前分位（40%）+近期趋势（35%）+Z-Score偏离度（25%）」计算0-100分。
    权重来自宏观金融研究（Howell 2020, Adrian &amp; Shin 2010等）及历史相关性分析。
    BTC对流动性beta约1.5x（Cross-Border Capital 2023），纳斯达克约1.05x，标普500约0.9x。
    本评分仅供参考，不构成投资建议。
</div>
'''

    def _build_html(self, summary: dict, charts_b64: dict, score_data: dict = None) -> str:
        j = summary.get("judgment", {})
        readings = summary.get("latest_readings", {})
        changes = summary.get("changes_summary", {})
        quality = summary.get("data_quality", {})
        details = summary.get("dimension_details", {})
        meta = summary.get("meta", {})

        regime_styles = {
            "TIGHTENING": {"bg": "#fee2e2", "border": "#ef4444", "icon": "🔴", "color": "#dc2626"},
            "LOCAL_DISTURBANCE": {"bg": "#fef3c7", "border": "#f59e0b", "icon": "🟡", "color": "#d97706"},
            "STABLE": {"bg": "#d1fae5", "border": "#10b981", "icon": "🟢", "color": "#059669"},
            "UNKNOWN": {"bg": "#e5e7eb", "border": "#6b7280", "icon": "⚪", "color": "#4b5563"},
        }
        rs = regime_styles.get(j.get("regime", "UNKNOWN"), regime_styles["UNKNOWN"])
        report_date = meta.get("report_date", "")
        gen_time = meta.get("generated_at", "")[:19]
        trading_days = meta.get("data_range", {}).get("trading_days", 0)

        # --- Dimension cards ---
        dim_names = {
            "net_liquidity": ("净流动性", "系统的水位。走弱 = 流动性收缩的源头信号"),
            "sofr": ("短端资金", "融资成本。走高 = 银行间借贷紧张"),
            "move_proxy": ("利率波动", "债市恐慌。走高 = 利率不确定性上升"),
            "carry_chain": ("套息链条", "全球杠杆方向。走弱 = 去杠杆压力"),
            "hy_oas": ("信用利差", "违约预期。走阔 = 信用市场开始定价风险"),
            "risk_assets": ("风险资产", "市场确认。走弱 = 流动性收紧已传导至市场"),
        }
        dim_cards = ""
        for dim, info in details.items():
            if not isinstance(info, dict):
                continue
            is_stress = info.get("stress") or info.get("weakening") or info.get("confirming_weakness")
            border = "#ef4444" if is_stress else "#10b981"
            bg = "#fef2f2" if is_stress else "#f0fdf4"
            status = "⚠️ STRESS" if is_stress else "✅ OK"
            name, desc = dim_names.get(dim, (dim, ""))
            dim_cards += (
                f'<div class="dim-card" style="background:{bg};border:2px solid {border}">'
                f'<div class="dim-card-title">{name} {status}</div>'
                f'<div class="dim-card-data">{info.get("detail", "")}</div>'
                f'<div class="dim-card-desc">{desc}</div></div>'
            )

        # --- Summary table ---
        metric_defs = [
            ("net_liquidity", "净流动性", "B", 1),
            ("sofr", "SOFR", "%", 4),
            ("hy_oas", "HY OAS", "%", 2),
            ("move_proxy", "MOVE Proxy", "", 1),
            ("vix", "VIX", "", 1),
            ("usdjpy", "USD/JPY", "", 1),
            ("carry_spread_bps", "套息利差", "bps", 0),
            ("curve_slope_bps", "曲线斜率", "bps", 0),
            ("spx", "S&P 500", "", 0),
            ("btc", "Bitcoin", "$", 0),
            ("dxy", "DXY", "", 1),
            ("us2y", "US 2Y", "%", 3),
            ("us10y", "US 10Y", "%", 3),
        ]
        bad_when_up = {"sofr", "hy_oas", "vix", "move_proxy", "dxy"}
        pct_indicators = {"spx", "btc", "usdjpy", "dxy"}

        table_rows = ""
        for key, label, unit, dec in metric_defs:
            r = readings.get(key, {})
            c = changes.get(key, {})
            val = r.get("value")
            val_str = f"{val:,.{dec}f}{unit}" if val is not None else "N/A"
            chg_5d = c.get("pct_5d", c.get("chg_5d"))
            if chg_5d is not None and key in pct_indicators and "pct_5d" in c:
                chg_str = f"{c['pct_5d']*100:+.1f}%"
            elif chg_5d is not None:
                chg_str = f"{chg_5d:+.2f}"
            else:
                chg_str = "N/A"
            if chg_5d is not None:
                if key in bad_when_up:
                    chg_color = "#dc2626" if chg_5d > 0 else "#059669" if chg_5d < 0 else "#6b7280"
                else:
                    chg_color = "#dc2626" if chg_5d < 0 else "#059669" if chg_5d > 0 else "#6b7280"
            else:
                chg_color = "#6b7280"
            zscore = r.get("zscore", "N/A")
            zscore_str = f"{zscore:+.2f}" if isinstance(zscore, (int, float)) else "N/A"
            pctl = r.get("percentile")
            pctl_str = f"{pctl*100:.0f}%" if pctl is not None else "N/A"
            sig = r.get("signal", "N/A")
            badge = self._signal_badge(sig) if sig != "N/A" else "N/A"
            table_rows += (
                f'<tr><td style="font-weight:600">{label}</td>'
                f'<td class="num">{val_str}</td>'
                f'<td class="num" style="color:{chg_color};font-weight:600">{chg_str}</td>'
                f'<td style="text-align:center">{zscore_str}</td>'
                f'<td style="text-align:center">{pctl_str}</td>'
                f'<td style="text-align:center">{badge}</td></tr>'
            )

        # --- Indicator sections (chart + explanation + interpretation) ---
        section_order = [
            "net_liquidity", "composite", "sofr", "move_proxy", "hy_oas",
            "usdjpy", "carry_spread", "curve_slope", "vix", "spx", "dxy", "btc",
        ]

        # Map knowledge keys to data keys for interpretation
        data_key_map = {
            "net_liquidity": "net_liquidity",
            "composite": "spx",  # composite uses spx for interpretation
            "sofr": "sofr",
            "move_proxy": "move_proxy",
            "hy_oas": "hy_oas",
            "usdjpy": "usdjpy",
            "carry_spread": "carry_spread_bps",
            "curve_slope": "curve_slope_bps",
            "vix": "vix",
            "spx": "spx",
            "dxy": "dxy",
            "btc": "btc",
        }

        sections_html = ""
        section_idx = 0
        for sec_key in section_order:
            kb = INDICATOR_KNOWLEDGE.get(sec_key)
            if not kb:
                continue
            chart_key = kb.get("chart_key", "")
            if chart_key not in charts_b64:
                continue

            section_idx += 1
            data_key = data_key_map.get(sec_key, sec_key)
            r = readings.get(data_key, {})
            val = r.get("value")
            sig = r.get("signal", "NEUTRAL")

            # Current value display
            if val is not None:
                if data_key in ("spx", "btc"):
                    val_display = f"{val:,.0f}"
                elif data_key in ("sofr", "hy_oas", "us2y", "us10y"):
                    val_display = f"{val:.4f}%"
                elif data_key in ("net_liquidity",):
                    val_display = f"{val:,.1f}B"
                elif data_key in ("carry_spread_bps", "curve_slope_bps"):
                    val_display = f"{val:,.0f} bps"
                else:
                    val_display = f"{val:,.2f}"
            else:
                val_display = "N/A"

            interpretation = self._interpret(data_key, readings, changes)

            sections_html += f'''
            <div class="section" id="sec-{sec_key}">
                <div class="section-header">
                    <div class="section-num">{section_idx}</div>
                    <div class="section-title-block">
                        <h2 class="section-title">{kb["title"]}</h2>
                        <div class="section-current">
                            当前: <strong>{val_display}</strong> {self._signal_badge(sig)}
                        </div>
                    </div>
                </div>

                <div class="section-body">
                    <div class="chart-area">
                        <img src="data:image/png;base64,{charts_b64[chart_key]}"
                             style="width:100%;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.1)"/>
                    </div>

                    <div class="explain-area">
                        <div class="explain-block">
                            <div class="explain-label">这是什么</div>
                            <div class="explain-text">{kb["what"]}</div>
                        </div>
                        <div class="explain-row">
                            <div class="explain-half good">
                                <div class="explain-label">▲ 偏高意味着</div>
                                <div class="explain-text">{kb["high_means"]}</div>
                            </div>
                            <div class="explain-half bad">
                                <div class="explain-label">▼ 偏低意味着</div>
                                <div class="explain-text">{kb["low_means"]}</div>
                            </div>
                        </div>
                        <div class="interpret-block">
                            <div class="explain-label">📊 当前解读</div>
                            <div class="explain-text">{interpretation}</div>
                        </div>
                    </div>
                </div>
            </div>'''

        # --- Quality section ---
        quality_rows = ""
        for k, v in quality.items():
            if not isinstance(v, dict):
                continue
            st = v.get("status", "?")
            cov = v.get("coverage", 0)
            dot = "#10b981" if st == "ok" else "#f59e0b" if st == "degraded" else "#ef4444"
            quality_rows += (
                f'<tr><td>{k}</td><td><span style="color:{dot}">●</span> {st}</td>'
                f'<td>{cov*100:.0f}%</td><td>{v.get("stale_days",0)}</td>'
                f'<td>{v.get("last_valid","N/A")}</td></tr>'
            )

        # --- Assemble full HTML ---
        return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>宏观流动性日报 | {report_date}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;background:#f8fafc;color:#1e293b;line-height:1.6;font-size:14px}}
.container{{max-width:1100px;margin:0 auto;padding:24px}}

/* Header */
.header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:28px;flex-wrap:wrap;gap:16px}}
.header h1{{font-size:26px;font-weight:800;letter-spacing:-0.5px}}
.header .sub{{color:#64748b;font-size:13px;margin-top:4px}}
.regime-box{{text-align:right}}
.regime-badge{{padding:10px 24px;border-radius:14px;font-size:20px;font-weight:800;display:inline-block}}

/* Judgment box */
.judgment-box{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 24px;margin-bottom:28px}}
.judgment-box .title{{font-weight:700;font-size:16px;margin-bottom:8px}}
.judgment-box .explanation{{font-size:15px;line-height:1.7}}
.judgment-box .meta-line{{margin-top:10px;font-size:13px;color:#64748b;display:flex;gap:16px;flex-wrap:wrap}}
.judgment-box .meta-tag{{background:#f1f5f9;padding:2px 10px;border-radius:6px}}

/* Dimension cards */
.dim-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin-bottom:28px}}
.dim-card{{border-radius:12px;padding:14px 16px}}
.dim-card-title{{font-weight:700;font-size:15px;margin-bottom:4px}}
.dim-card-data{{font-size:13px;color:#374151;font-family:"SF Mono",Monaco,monospace}}
.dim-card-desc{{font-size:12px;color:#64748b;margin-top:6px;font-style:italic}}

/* Table */
.data-table{{background:#fff;border-radius:12px;overflow:hidden;border:1px solid #e2e8f0;margin-bottom:28px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#f1f5f9;padding:10px 12px;text-align:left;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#64748b;border-bottom:2px solid #e2e8f0}}
td{{padding:9px 12px;border-bottom:1px solid #f1f5f9}}
tr:hover{{background:#fafbfc}}
td.num{{text-align:right;font-variant-numeric:tabular-nums;font-family:"SF Mono",Monaco,monospace}}

/* Sections */
h2.divider{{font-size:20px;font-weight:800;margin:40px 0 20px;padding-bottom:8px;border-bottom:3px solid #1e293b;letter-spacing:-0.3px}}

.section{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:24px;margin-bottom:24px}}
.section-header{{display:flex;align-items:center;gap:16px;margin-bottom:16px}}
.section-num{{width:36px;height:36px;border-radius:50%;background:#1e293b;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:16px;flex-shrink:0}}
.section-title{{font-size:17px;font-weight:700;margin:0}}
.section-current{{font-size:13px;color:#64748b;margin-top:2px}}

.section-body{{}}
.chart-area{{margin-bottom:16px}}
.explain-area{{}}
.explain-block{{margin-bottom:14px}}
.explain-label{{font-weight:700;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;color:#64748b;margin-bottom:4px}}
.explain-text{{font-size:13.5px;line-height:1.7;color:#374151}}
.explain-row{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}}
.explain-half{{padding:12px 14px;border-radius:10px;font-size:13px;line-height:1.6}}
.explain-half.good{{background:#f0fdf4;border-left:3px solid #10b981}}
.explain-half.bad{{background:#fef2f2;border-left:3px solid #ef4444}}
.interpret-block{{background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:14px 16px}}
.interpret-block .explain-text{{color:#1e40af}}

/* Quality */
.quality-section{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 24px;margin-bottom:28px}}
.quality-section table{{font-size:12px}}

/* Footer */
.footer{{margin-top:40px;padding:16px 0;border-top:1px solid #e2e8f0;text-align:center;font-size:12px;color:#94a3b8}}

/* Nav */
.toc{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px 20px;margin-bottom:28px}}
.toc-title{{font-weight:700;font-size:13px;color:#64748b;margin-bottom:8px}}
.toc-links{{display:flex;flex-wrap:wrap;gap:8px}}
.toc-link{{display:inline-block;padding:4px 12px;background:#f1f5f9;border-radius:6px;font-size:12px;color:#475569;text-decoration:none;transition:all .15s}}
.toc-link:hover{{background:#e2e8f0;color:#1e293b}}

.note{{font-size:12px;color:#94a3b8;padding:8px 12px;background:#f8fafc;border-radius:6px;margin-top:8px}}

/* ===== Score Hero ===== */
.score-hero{{background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);border-radius:18px;padding:32px;margin-bottom:24px;display:flex;gap:32px;flex-wrap:wrap;color:#fff}}
.score-hero-left{{flex:0 0 260px;text-align:center}}
.score-hero-title{{font-size:13px;text-transform:uppercase;letter-spacing:1.5px;color:#94a3b8;margin-bottom:8px}}
.score-hero-number{{font-size:72px;font-weight:900;line-height:1;letter-spacing:-3px}}
.score-hero-tier{{display:inline-block;padding:6px 20px;border-radius:12px;font-size:18px;font-weight:800;margin-top:8px}}
.score-hero-gauge{{margin-top:8px}}
.score-hero-right{{flex:1;min-width:300px}}
.advice-card{{}}
.advice-position{{font-size:24px;font-weight:800;margin-bottom:8px}}
.advice-detail{{font-size:14px;line-height:1.8;color:#cbd5e1;margin-bottom:16px}}
.advice-actions{{display:flex;flex-direction:column;gap:6px;margin-bottom:14px}}
.advice-action{{font-size:13px;padding:8px 14px;background:rgba(255,255,255,0.06);border-radius:8px;border-left:3px solid #3b82f6;color:#e2e8f0}}
.advice-risk{{font-size:13px;color:#fca5a5;margin-top:8px;padding:6px 12px;background:rgba(239,68,68,0.1);border-radius:6px}}
.advice-catalyst{{font-size:13px;color:#86efac;margin-top:6px;padding:6px 12px;background:rgba(34,197,94,0.1);border-radius:6px}}

/* Asset outlook */
.asset-outlook{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 24px;margin-bottom:24px}}
.asset-outlook-title{{font-size:16px;font-weight:700;margin-bottom:14px}}
.asset-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:14px}}
.asset-card{{border:1px solid #e2e8f0;border-radius:12px;padding:16px;text-align:center}}
.asset-icon{{font-size:28px;margin-bottom:4px}}
.asset-name{{font-size:14px;font-weight:700;color:#374151}}
.asset-score{{font-size:36px;font-weight:900;margin:4px 0}}
.asset-tier{{display:inline-block;padding:3px 14px;border-radius:8px;font-size:13px;font-weight:700;margin-bottom:8px}}
.asset-note{{font-size:11px;color:#64748b;line-height:1.5}}

/* Score breakdown */
.score-breakdown{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:20px 24px;margin-bottom:24px}}
.score-breakdown-title{{font-size:16px;font-weight:700;margin-bottom:4px}}
.score-breakdown-subtitle{{font-size:12px;color:#64748b;margin-bottom:16px}}
.score-bars{{display:flex;flex-direction:column;gap:8px;margin-bottom:18px}}
.score-bar-row{{display:flex;align-items:center;gap:10px}}
.score-bar-label{{width:120px;font-size:13px;font-weight:600;color:#374151;text-align:right;flex-shrink:0}}
.score-bar-weight{{font-size:11px;color:#94a3b8;font-weight:400}}
.score-bar-track{{flex:1;height:20px;background:#f1f5f9;border-radius:10px;overflow:hidden}}
.score-bar-fill{{height:100%;border-radius:10px;transition:width .5s}}
.score-bar-val{{width:32px;font-size:14px;font-weight:800;text-align:right;font-variant-numeric:tabular-nums}}
.score-bar-signal{{width:32px;font-size:12px;font-weight:600;text-align:center}}
.score-factors{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
.factors-col{{}}
.factors-title{{font-size:13px;font-weight:700;margin-bottom:8px}}
.factor-item{{font-size:12px;padding:4px 0;color:#374151}}
.factor-item.bull{{color:#16a34a}}
.factor-item.bear{{color:#dc2626}}
.score-note{{font-size:11px;color:#94a3b8;padding:10px 14px;background:#f8fafc;border-radius:8px;line-height:1.6;margin-bottom:28px}}

@media print {{
    .section {{ break-inside: avoid; }}
    .toc {{ display: none; }}
}}
</style>
</head>
<body>
<div class="container">

<!-- ============ HEADER ============ -->
<div class="header">
    <div>
        <h1>宏观流动性日报</h1>
        <div class="sub">{report_date} | 覆盖 {trading_days} 个交易日 | 生成于 {gen_time}</div>
    </div>
    <div class="regime-box">
        <div style="font-size:11px;color:#64748b;margin-bottom:4px;text-transform:uppercase;letter-spacing:1px">Regime</div>
        <div class="regime-badge" style="background:{rs['bg']};border:2px solid {rs['border']};color:{rs['color']}">
            {rs['icon']} {j.get('regime_cn','?')}
        </div>
        <div style="font-size:12px;color:#64748b;margin-top:4px">置信度: <strong>{j.get('confidence','?')}</strong></div>
    </div>
</div>

{self._build_score_section(score_data) if score_data else ''}

<!-- ============ JUDGMENT ============ -->
<div class="judgment-box">
    <div class="title">综合研判</div>
    <div class="explanation">{j.get('explanation','')}</div>
    <div class="meta-line">
        <span class="meta-tag">净流动性走弱: <strong>{'是' if j.get('net_liquidity_weakening') else '否'}</strong></span>
        <span class="meta-tag">确认维度: <strong>{j.get('stress_count',0)}</strong> 个 ({', '.join(j.get('stress_dimensions',[])) or '无'})</span>
        <span class="meta-tag">风险资产确认: <strong>{'是' if j.get('risk_asset_confirming') else '否'}</strong></span>
    </div>
    <div class="note" style="margin-top:12px">
        判断规则：只有"净流动性走弱 + 至少2个确认维度同步走弱"才判定为"明显趋紧"。单一指标走坏 = 局部扰动。风险资产未确认 = 前置信号出现但市场确认不足。数据缺失则保守判断。
    </div>
</div>

<!-- ============ DIMENSION OVERVIEW ============ -->
<h2 class="divider">一、维度总览</h2>
<div class="dim-grid">{dim_cards}</div>

<!-- ============ SUMMARY TABLE ============ -->
<h2 class="divider">二、指标速览</h2>
<div class="data-table">
<table>
    <thead><tr>
        <th>指标</th><th style="text-align:right">最新值</th><th style="text-align:right">5日变动</th>
        <th style="text-align:center">Z-Score</th><th style="text-align:center">分位</th><th style="text-align:center">信号</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
</table>
</div>
<div class="note">Z-Score: 基于近60个交易日滚动计算，衡量当前值偏离均值的程度（>1.5为显著偏离）。分位: 近252个交易日排名百分位。信号: STRESS=压力 / TIGHT=偏紧 / NEUTRAL=中性 / EASING=宽松。MOVE Proxy 基于 VIX×利率波动率构建（非原始ICE MOVE）。JP 2Y为月频插值。</div>

<!-- ============ NAV ============ -->
<h2 class="divider">三、逐项分析</h2>
<div class="toc">
    <div class="toc-title">快速导航</div>
    <div class="toc-links">
        <a href="#sec-net_liquidity" class="toc-link">净流动性</a>
        <a href="#sec-composite" class="toc-link">流动性vs风险资产</a>
        <a href="#sec-sofr" class="toc-link">SOFR</a>
        <a href="#sec-move_proxy" class="toc-link">MOVE Proxy</a>
        <a href="#sec-hy_oas" class="toc-link">HY OAS</a>
        <a href="#sec-usdjpy" class="toc-link">USD/JPY</a>
        <a href="#sec-carry_spread" class="toc-link">套息利差</a>
        <a href="#sec-curve_slope" class="toc-link">曲线斜率</a>
        <a href="#sec-vix" class="toc-link">VIX</a>
        <a href="#sec-spx" class="toc-link">S&P 500</a>
        <a href="#sec-dxy" class="toc-link">DXY</a>
        <a href="#sec-btc" class="toc-link">Bitcoin</a>
    </div>
</div>

<!-- ============ INDICATOR SECTIONS ============ -->
{sections_html}

<!-- ============ DATA QUALITY ============ -->
<h2 class="divider">四、数据源状态</h2>
<div class="quality-section">
<table>
    <thead><tr><th>指标</th><th>状态</th><th>覆盖率</th><th>滞后(天)</th><th>最后有效日</th></tr></thead>
    <tbody>{quality_rows}</tbody>
</table>
<div class="note">覆盖率 = 非空观测 / 总交易日。滞后 = 最后有效值距面板末日天数。超过3天标记为stale，判断引擎自动降级置信度。</div>
</div>

<!-- ============ FOOTER ============ -->
<div class="footer">
    宏观流动性日报 | 数据源: FRED API + Yahoo Finance | Python自动化生成<br>
    本报告仅供研究参考，不构成投资建议。市场有风险，投资需谨慎。
</div>

</div>
</body>
</html>'''
