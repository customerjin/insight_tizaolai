"""
services/brief_service.py - Daily brief orchestrator.

Coordinates all services and providers to produce the final daily brief.
This is the main entry point for generating the daily analysis.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class BriefService:
    """Orchestrates the daily brief generation pipeline."""

    def __init__(self, config: dict):
        self.config = config

    def generate(self, macro_data: dict = None) -> dict:
        """
        Generate the complete daily brief.

        Args:
            macro_data: Existing macro dashboard data (score, judgment, etc.)
                        Passed from the main pipeline to integrate with existing system.

        Returns: Complete daily_brief dict ready for JSON export.
        """
        logger.info("=" * 50)
        logger.info("DAILY BRIEF GENERATION")
        logger.info("=" * 50)

        result = {
            'status': 'ok',
            'generated_at': datetime.now().isoformat(),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'update_time': datetime.now().strftime('%H:%M'),
        }

        errors = []

        # 1. Fetch market indices
        logger.info("Brief Step 1: Fetching market indices...")
        try:
            from providers.market_provider import MarketProvider
            from services.market_service import MarketService

            market_provider = MarketProvider(self.config)
            raw_market = market_provider.fetch_all_indices()

            market_service = MarketService(self.config)
            market_data = market_service.process(raw_market)
            result['market'] = market_data
            logger.info(f"Market: {market_data['status']} ({len(market_data['indices'])} indices)")
        except Exception as e:
            logger.error(f"Market fetch failed: {e}")
            errors.append(f"market: {e}")
            result['market'] = {
                'status': 'error', 'indices': [], 'summary': '行情数据获取失败',
                'update_time': datetime.now().strftime('%H:%M'),
                'status_text': '获取失败',
            }

        # 2. Fetch news
        logger.info("Brief Step 2: Fetching news...")
        try:
            from providers.news_provider import NewsProvider
            from services.news_service import NewsService

            news_provider = NewsProvider(self.config)
            raw_news = news_provider.fetch_news()

            news_service = NewsService(self.config)
            news_data = news_service.process(raw_news)
            result['news'] = news_data
            logger.info(f"News: {news_data['status']} ({len(news_data.get('top5', []))} top events)")
        except Exception as e:
            logger.error(f"News fetch failed: {e}")
            errors.append(f"news: {e}")
            result['news'] = {'status': 'error', 'top5': [], 'all_articles': []}

        # 3. Detect star stock movers
        logger.info("Brief Step 3: Detecting star stock movers...")
        try:
            from services.movers_service import MoversService

            movers_service = MoversService(self.config)
            movers_data = movers_service.detect_movers()
            result['movers'] = movers_data
            g_count = len(movers_data.get('gainers', []))
            l_count = len(movers_data.get('losers', []))
            logger.info(f"Movers: {movers_data['status']} ({g_count} gainers, {l_count} losers)")
        except Exception as e:
            logger.error(f"Movers detection failed: {e}")
            errors.append(f"movers: {e}")
            result['movers'] = {'status': 'error', 'gainers': [], 'losers': []}

        # 4. Generate AI commentary
        logger.info("Brief Step 4: Generating analysis...")
        try:
            from providers.analysis_provider import AnalysisProvider
            from storage.snapshot_store import SnapshotStore

            analysis_provider = AnalysisProvider(self.config)
            analysis = analysis_provider.generate_commentary(
                market_data=result.get('market', {}),
                news_data=result.get('news', {}),
                movers_data=result.get('movers', {}),
                macro_data=macro_data,
            )
            result['analysis'] = {
                'commentary': analysis.get('commentary', {}),
                'outlook': analysis.get('outlook', []),
                'source': analysis.get('source', 'unknown'),
                'status': analysis.get('status', 'ok'),
            }

            # Save snapshot for audit
            try:
                snapshot_store = SnapshotStore(self.config)
                snapshot_store.save(
                    'daily_analysis',
                    input_data={
                        'market': result.get('market', {}).get('summary', ''),
                        'news_count': len(result.get('news', {}).get('top5', [])),
                        'movers_count': len(result.get('movers', {}).get('gainers', [])),
                    },
                    output_data=analysis,
                    metadata={'source': analysis.get('source', '')},
                )
            except Exception as snap_err:
                logger.warning(f"Snapshot save failed: {snap_err}")

            logger.info(f"Analysis: {analysis.get('status')} (source: {analysis.get('source')})")
        except Exception as e:
            logger.error(f"Analysis generation failed: {e}")
            errors.append(f"analysis: {e}")
            result['analysis'] = {
                'commentary': {
                    'main_theme': '分析生成失败，请查看行情数据',
                    'risk_points': '暂无',
                    'watch_next': '暂无',
                },
                'outlook': [],
                'source': 'error',
                'status': 'error',
            }

        # 5. AI-interpret English content to Chinese (解读, not literal translation)
        api_key = self.config.get('daily_brief', {}).get('analysis', {}).get('api_key', '')
        import os
        api_key = api_key or os.environ.get('ANALYSIS_API_KEY', '')
        if api_key:
            logger.info("Brief Step 5: AI interpreting content to Chinese...")
            try:
                result['news'], result['movers'] = self._translate_content(
                    result.get('news', {}), result.get('movers', {}), api_key
                )
                logger.info("AI interpretation: ok")
            except Exception as e:
                logger.warning(f"AI interpretation failed (using original): {e}")

        # 6. Set overall status
        if not errors:
            result['status'] = 'ok'
            result['data_status'] = '数据正常'
        elif len(errors) < 3:
            result['status'] = 'partial'
            result['data_status'] = f'部分成功 (失败: {len(errors)}项)'
        else:
            result['status'] = 'degraded'
            result['data_status'] = '降级运行'

        result['errors'] = errors
        result['disclaimer'] = '以上内容为信息整理与研究辅助，不构成投资建议。市场有风险，投资需谨慎。'

        logger.info(f"Daily brief complete: {result['status']} ({len(errors)} errors)")
        return result

    def _translate_content(self, news_data: dict, movers_data: dict, api_key: str):
        """AI-interpret English news and movers reasons into Chinese financial journalism style."""
        import json
        import requests as req

        # Collect all texts to interpret in one API call
        items = []

        # News
        events = news_data.get('top5', news_data.get('events', []))
        for i, evt in enumerate(events):
            title = evt.get('title', '')
            summary = evt.get('summary', '')
            source = evt.get('source', '')
            # Skip if already Chinese
            if title and not self._is_chinese(title):
                items.append({"id": f"news_title_{i}", "type": "news_title", "text": title, "source": source})
            if summary and summary != title and not self._is_chinese(summary):
                items.append({"id": f"news_summary_{i}", "type": "news_summary", "text": summary, "source": source})

        # Movers reasons
        for group in ['gainers', 'losers']:
            for j, stock in enumerate(movers_data.get(group, [])):
                reason = stock.get('reason', {})
                if isinstance(reason, dict):
                    text = reason.get('text', '')
                elif isinstance(reason, str):
                    text = reason
                else:
                    text = ''
                stock_name = stock.get('name', stock.get('symbol', ''))
                change_pct = stock.get('change_pct', 0)
                direction = '上涨' if group == 'gainers' else '下跌'
                if text and text != '暂无可靠原因' and not self._is_chinese(text):
                    items.append({
                        "id": f"mover_{group}_{j}", "type": "mover_reason",
                        "text": text, "stock": stock_name,
                        "change": f"{direction}{abs(change_pct):.1f}%",
                    })

        if not items:
            return news_data, movers_data

        # Build interpretation prompt — 解读而非翻译
        texts_json = json.dumps(items, ensure_ascii=False)
        prompt = f"""你是一位资深中文财经编辑。请对以下JSON数组中的英文内容进行**解读和改写**（不是逐字翻译）。

规则：
1. **新闻标题(news_title)**：改写为中文财经新闻标题风格，简洁有力，突出核心信息和市场影响。可以补充隐含的市场意义。
2. **新闻摘要(news_summary)**：用1-2句中文概括事件要点和对投资者的影响，像财经媒体的快讯解读。
3. **异动原因(mover_reason)**：结合stock和change信息，用中文解读为什么这只股票会有这样的表现，写成一句话的分析师点评风格。

保持id不变，将text替换为解读后的中文。直接返回JSON数组，不要其他文字。

{texts_json}"""

        base_url = self.config.get('daily_brief', {}).get('analysis', {}).get('base_url', 'https://openrouter.ai/api/v1')
        model = self.config.get('daily_brief', {}).get('analysis', {}).get('model', 'google/gemini-2.0-flash-001')

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://invest-wine.vercel.app",
            "X-Title": "Macro Liquidity Daily Brief",
        }

        resp = req.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 3000,
            },
            timeout=60,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]

        # Parse response
        import re
        json_match = re.search(r'\[[\s\S]*\]', content)
        if not json_match:
            logger.warning("Translation response not valid JSON array")
            return news_data, movers_data

        translations = json.loads(json_match.group())
        trans_map = {t['id']: t['text'] for t in translations if 'id' in t and 'text' in t}

        # Apply translations to news
        for i, evt in enumerate(events):
            if f"news_title_{i}" in trans_map:
                evt['title'] = trans_map[f"news_title_{i}"]
            if f"news_summary_{i}" in trans_map:
                evt['summary'] = trans_map[f"news_summary_{i}"]

        # Update news_data
        if 'top5' in news_data:
            news_data['top5'] = events
        elif 'events' in news_data:
            news_data['events'] = events

        # Apply translations to movers
        for group in ['gainers', 'losers']:
            for j, stock in enumerate(movers_data.get(group, [])):
                key = f"mover_{group}_{j}"
                if key in trans_map:
                    reason = stock.get('reason', {})
                    if isinstance(reason, dict):
                        reason['text'] = trans_map[key]
                    else:
                        stock['reason'] = {'text': trans_map[key], 'url': '', 'source': '', 'confidence': 'translated'}

        logger.info(f"AI interpreted {len(trans_map)} items to Chinese")
        return news_data, movers_data

    @staticmethod
    def _is_chinese(text: str) -> bool:
        """Check if text is predominantly Chinese."""
        if not text:
            return False
        chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
        return chinese_chars / max(len(text), 1) > 0.3
