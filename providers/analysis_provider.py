"""
providers/analysis_provider.py - AI analysis provider.

Supports multiple LLM backends (OpenAI, Anthropic) with rule-based fallback.
Generates market commentary, investment outlook, and stock reason attribution.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from .base import BaseProvider

logger = logging.getLogger(__name__)


class AnalysisProvider(BaseProvider):
    """Generates AI-powered market analysis with rule-based fallback."""

    def __init__(self, config: dict):
        brief_cfg = config.get('daily_brief', {})
        analysis_cfg = brief_cfg.get('analysis', {})
        cache_ttl = brief_cfg.get('cache', {}).get('analysis_ttl', 3600)
        cache_dir = config.get('cache', {}).get('dir', 'cache')

        super().__init__(config, cache_dir=f"{cache_dir}/brief/analysis", cache_ttl=cache_ttl)

        self.api_key = analysis_cfg.get('api_key', '') or os.environ.get('ANALYSIS_API_KEY', '')
        self.model = analysis_cfg.get('model', 'google/gemini-2.0-flash-001')
        self.base_url = analysis_cfg.get('base_url', 'https://openrouter.ai/api/v1')

        # Auto-detect provider from key/config
        configured_provider = analysis_cfg.get('provider', 'none')
        if configured_provider != 'none':
            self.provider = configured_provider
        elif self.api_key.startswith('sk-or-'):
            self.provider = 'openrouter'
        elif self.api_key.startswith('sk-ant-'):
            self.provider = 'anthropic'
        elif self.api_key:
            self.provider = 'openai'
        else:
            self.provider = 'none'

        if self.provider != 'none':
            logger.info(f"Analysis provider: {self.provider} (model: {self.model})")

    def generate_commentary(self, market_data: dict, news_data: dict,
                           movers_data: dict, macro_data: dict = None) -> dict:
        """
        Generate market commentary and investment outlook.

        Args:
            market_data: From market_service (index performance)
            news_data: From news_service (top events)
            movers_data: From movers_service (star stocks)
            macro_data: From existing macro dashboard (score, judgment)

        Returns: {commentary, outlook, status}
        """
        if self.provider in ('openrouter', 'openai') and self.api_key:
            try:
                return self._generate_with_openai_compatible(market_data, news_data, movers_data, macro_data)
            except Exception as e:
                logger.warning(f"{self.provider} generation failed: {e}, falling back to rules")

        if self.provider == 'anthropic' and self.api_key:
            try:
                return self._generate_with_anthropic(market_data, news_data, movers_data, macro_data)
            except Exception as e:
                logger.warning(f"Anthropic generation failed: {e}, falling back to rules")

        # Rule-based fallback (always available)
        return self._generate_rule_based(market_data, news_data, movers_data, macro_data)

    def _build_prompt(self, market_data: dict, news_data: dict,
                      movers_data: dict, macro_data: dict) -> str:
        """Build the analysis prompt from structured data."""
        sections = []

        # Market indices
        sections.append("## 今日主要市场表现")
        indices = market_data.get('data', [])
        for idx in indices:
            if idx.get('price') is not None:
                chg = idx.get('change_pct', 0) or 0
                direction = "上涨" if chg > 0 else "下跌" if chg < 0 else "持平"
                sections.append(f"- {idx['name']}: {idx['price']} ({'+' if chg > 0 else ''}{chg}%, {direction}, {idx.get('trading_status', '')})")

        # Top events
        if news_data.get('top5'):
            sections.append("\n## 投资圈重要事件")
            for i, evt in enumerate(news_data['top5'], 1):
                sections.append(f"{i}. {evt.get('title', '')} (来源: {evt.get('source', '未知')})")

        # Star stock movers
        if movers_data:
            sections.append("\n## 明星股异动")
            for direction in ['gainers', 'losers']:
                items = movers_data.get(direction, [])
                if items:
                    label = "涨幅榜" if direction == 'gainers' else "跌幅榜"
                    sections.append(f"\n### {label}")
                    for s in items[:5]:
                        sections.append(f"- {s.get('name', '')} ({s.get('symbol', '')}): {s.get('change_pct', 0):.1f}%")
                        if s.get('reason'):
                            sections.append(f"  原因: {s['reason']}")

        # Macro context
        if macro_data:
            sections.append("\n## 宏观流动性背景")
            if macro_data.get('score'):
                sections.append(f"- 流动性综合评分: {macro_data['score'].get('composite', 'N/A')}/100")
                sections.append(f"- 研判结果: {macro_data.get('judgment', {}).get('regime_cn', 'N/A')}")

        data_text = "\n".join(sections)

        prompt = f"""你是一位资深投研分析师，请基于以下今日市场数据，生成简洁的每日投研分析报告。

{data_text}

请严格按以下JSON格式输出，不要添加任何其他文字：

{{
  "commentary": {{
    "main_theme": "今日市场主线（1-2句话）",
    "risk_points": "风险点（1-2个具体风险）",
    "watch_next": "下一交易日观察点（值得跟踪什么）"
  }},
  "outlook": [
    {{
      "sector": "板块名称",
      "direction": "偏多/偏空/中性",
      "logic": ["核心逻辑1", "核心逻辑2"],
      "watch_stocks": ["可选：关注标的"],
      "trigger": "触发条件",
      "risk": "风险点"
    }}
  ]
}}

要求：
1. 语言简洁真实，不说空话
2. 每个观点都要有数据或新闻支撑
3. 投资动向必须给出看多和看空的触发条件
4. 明确这是信息整理与研究辅助，不是投资建议"""

        return prompt

    def _generate_with_openai_compatible(self, market_data, news_data, movers_data, macro_data) -> dict:
        """Generate analysis using OpenAI-compatible API (OpenRouter, OpenAI, etc)."""
        import requests as req

        prompt = self._build_prompt(market_data, news_data, movers_data, macro_data or {})

        # Determine API URL
        if self.provider == 'openrouter':
            url = f"{self.base_url}/chat/completions"
        else:
            url = "https://api.openai.com/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        # OpenRouter requires HTTP-Referer
        if self.provider == 'openrouter':
            headers["HTTP-Referer"] = "https://invest-wine.vercel.app"
            headers["X-Title"] = "Macro Liquidity Daily Brief"

        resp = req.post(
            url,
            headers=headers,
            json={
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "max_tokens": 2000,
            },
            timeout=60,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]

        parsed = self._parse_json_response(content)
        parsed['status'] = 'ok'
        parsed['source'] = f'{self.provider}:{self.model}'
        parsed['raw_prompt'] = prompt
        parsed['raw_response'] = content
        return parsed

    def _generate_with_anthropic(self, market_data, news_data, movers_data, macro_data) -> dict:
        """Generate analysis using Anthropic API."""
        import requests as req

        prompt = self._build_prompt(market_data, news_data, movers_data, macro_data or {})

        resp = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": self.model if 'claude' in self.model else "claude-sonnet-4-5-20250929",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]

        parsed = self._parse_json_response(content)
        parsed['status'] = 'ok'
        parsed['source'] = f'anthropic:{self.model}'
        parsed['raw_prompt'] = prompt
        parsed['raw_response'] = content
        return parsed

    def _parse_json_response(self, text: str) -> dict:
        """Extract JSON from AI response text."""
        # Try to find JSON block
        import re
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        # Return as raw text
        return {'commentary': {'main_theme': text[:500]}, 'outlook': []}

    def _generate_rule_based(self, market_data: dict, news_data: dict,
                            movers_data: dict, macro_data: dict = None) -> dict:
        """Rule-based analysis generation (no API needed)."""
        indices = market_data.get('data', [])

        # Determine market mood
        changes = [idx.get('change_pct', 0) or 0 for idx in indices if idx.get('change_pct') is not None]
        avg_change = sum(changes) / len(changes) if changes else 0
        positive_count = sum(1 for c in changes if c > 0)
        negative_count = sum(1 for c in changes if c < 0)

        # Main theme
        if avg_change > 1.5:
            mood = "全球市场普涨"
            mood_detail = "风险偏好回升"
        elif avg_change > 0.3:
            mood = "市场温和上行"
            mood_detail = "多数指数录得涨幅"
        elif avg_change < -1.5:
            mood = "全球市场承压"
            mood_detail = "避险情绪升温"
        elif avg_change < -0.3:
            mood = "市场小幅回调"
            mood_detail = "部分指数走弱"
        else:
            mood = "市场横盘整理"
            mood_detail = "多空博弈胶着"

        # Build index details for theme
        idx_details = []
        for idx in indices:
            if idx.get('change_pct') is not None:
                name = idx.get('name', idx.get('symbol', ''))
                chg = idx['change_pct']
                idx_details.append(f"{name}{'+' if chg > 0 else ''}{chg:.1f}%")

        main_theme = f"{mood}，{mood_detail}。" + "、".join(idx_details[:3]) + "。"

        # Risk points
        risks = []
        for idx in indices:
            chg = idx.get('change_pct', 0) or 0
            if chg < -2:
                risks.append(f"{idx.get('name', '')}大幅下跌{chg:.1f}%，需关注是否持续")
            if chg > 5:
                risks.append(f"{idx.get('name', '')}涨幅过大(+{chg:.1f}%)，注意短期回调风险")

        if macro_data and macro_data.get('judgment', {}).get('regime') == 'TIGHTENING':
            risks.append("宏观流动性趋紧，系统性风险上升")

        if not risks:
            if avg_change < 0:
                risks.append("市场整体偏弱，注意仓位控制")
            else:
                risks.append("暂无明显风险信号，但需持续关注宏观数据")

        # Watch next
        watch_items = []
        top_events = news_data.get('top5', [])
        if top_events:
            watch_items.append(f"关注: {top_events[0].get('title', '重要新闻')[:50]}")
        if any(abs(idx.get('change_pct', 0) or 0) > 2 for idx in indices):
            watch_items.append("关注大幅波动指数能否企稳")

        if not watch_items:
            watch_items.append("关注下一交易日开盘表现及成交量变化")

        # Investment outlook (rule-based)
        outlook = []

        # Tech sector
        tech_idx = next((idx for idx in indices if 'NDX' in str(idx.get('symbol', '')) or 'Nasdaq' in str(idx.get('name', ''))), None)
        if tech_idx and tech_idx.get('change_pct') is not None:
            tech_chg = tech_idx['change_pct']
            outlook.append({
                'sector': '科技/AI',
                'direction': '偏多' if tech_chg > 0 else '偏空' if tech_chg < -1 else '中性',
                'logic': [
                    f"纳指{'上涨' if tech_chg > 0 else '下跌'}{abs(tech_chg):.1f}%",
                    "AI/半导体板块持续受资金关注" if tech_chg > 0 else "短期获利了结压力",
                ],
                'watch_stocks': ['NVDA', 'MSFT', 'AAPL'],
                'trigger': f"看多触发: 纳指站稳前高; 看空触发: 跌破20日均线",
                'risk': '估值偏高，对利率敏感',
            })

        # A-share / China
        cn_idx = next((idx for idx in indices if '沪深' in str(idx.get('name', '')) or '上证' in str(idx.get('name', ''))), None)
        if cn_idx and cn_idx.get('change_pct') is not None:
            cn_chg = cn_idx['change_pct']
            outlook.append({
                'sector': 'A股/中概',
                'direction': '偏多' if cn_chg > 0.5 else '偏空' if cn_chg < -0.5 else '中性',
                'logic': [
                    f"{cn_idx['name']}{'+' if cn_chg > 0 else ''}{cn_chg:.1f}%",
                    "政策面持续释放积极信号" if cn_chg > 0 else "市场等待更多催化剂",
                ],
                'watch_stocks': [],
                'trigger': '看多触发: 成交量放大+政策利好; 看空触发: 外资持续流出',
                'risk': '地缘政治风险、房地产市场不确定性',
            })

        # Crypto
        btc_idx = next((idx for idx in indices if 'BTC' in str(idx.get('symbol', ''))), None)
        if btc_idx and btc_idx.get('change_pct') is not None:
            btc_chg = btc_idx['change_pct']
            outlook.append({
                'sector': '加密货币',
                'direction': '偏多' if btc_chg > 1 else '偏空' if btc_chg < -2 else '中性',
                'logic': [
                    f"BTC{'+' if btc_chg > 0 else ''}{btc_chg:.1f}%",
                    "机构资金持续流入" if btc_chg > 0 else "短期获利盘抛压",
                ],
                'watch_stocks': ['BTC', 'ETH'],
                'trigger': '看多触发: 突破前高+ETF资金净流入; 看空触发: 跌破关键支撑',
                'risk': '监管政策不确定性、流动性敏感',
            })

        return {
            'status': 'ok',
            'source': 'rule_based',
            'commentary': {
                'main_theme': main_theme,
                'risk_points': '; '.join(risks[:3]),
                'watch_next': '; '.join(watch_items[:3]),
            },
            'outlook': outlook,
            'timestamp': datetime.now().isoformat(),
        }

    # BaseProvider interface
    def _fetch_impl(self, key: str, **kwargs) -> dict:
        return self.generate_commentary(
            kwargs.get('market_data', {}),
            kwargs.get('news_data', {}),
            kwargs.get('movers_data', {}),
            kwargs.get('macro_data', {}),
        )
