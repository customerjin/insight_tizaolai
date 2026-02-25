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

        # 5. Set overall status
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
