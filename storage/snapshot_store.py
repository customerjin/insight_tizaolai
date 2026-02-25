"""
storage/snapshot_store.py - AI input/output snapshot storage.

Saves raw input (prompt) and output (response) for every AI-generated analysis,
enabling post-hoc hallucination detection and audit trail.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SnapshotStore:
    """Stores snapshots of AI analysis inputs and outputs."""

    def __init__(self, config: dict):
        base_dir = Path(config.get('output', {}).get('base_dir', 'output'))
        self.snapshot_dir = base_dir / 'snapshots'
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def save(self, snapshot_type: str, input_data: dict, output_data: dict,
             metadata: dict = None) -> str:
        """
        Save a snapshot of AI analysis.

        Args:
            snapshot_type: e.g., 'market_commentary', 'investment_outlook'
            input_data: The raw data fed to the AI model
            output_data: The AI model's response
            metadata: Extra metadata (model, tokens, etc.)

        Returns:
            Path to the saved snapshot file.
        """
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{snapshot_type}_{ts}.json"
        filepath = self.snapshot_dir / filename

        snapshot = {
            'type': snapshot_type,
            'timestamp': datetime.now().isoformat(),
            'input': input_data,
            'output': output_data,
            'metadata': metadata or {},
        }

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Snapshot saved: {filepath}")

            # Cleanup old snapshots (keep last 30 per type)
            self._cleanup(snapshot_type, keep=30)

            return str(filepath)
        except Exception as e:
            logger.error(f"Snapshot save error: {e}")
            return ""

    def _cleanup(self, snapshot_type: str, keep: int = 30):
        """Remove old snapshots, keeping the most recent N."""
        pattern = f"{snapshot_type}_*.json"
        files = sorted(self.snapshot_dir.glob(pattern), key=lambda f: f.stat().st_mtime)
        for old_file in files[:-keep]:
            try:
                old_file.unlink()
            except Exception:
                pass

    def get_latest(self, snapshot_type: str) -> dict:
        """Get the most recent snapshot of a given type."""
        pattern = f"{snapshot_type}_*.json"
        files = sorted(self.snapshot_dir.glob(pattern), key=lambda f: f.stat().st_mtime)
        if not files:
            return {}
        try:
            with open(files[-1], 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
