"""Append-only JSONL log of risk scores per report run (local + CI).

File: ``data/risk_score_history.jsonl`` under the project root (``financial-agent/``).
The parent ``data/`` directory is gitignored in the monorepo — history stays on disk
unless you change ignore rules or copy the file elsewhere.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ..analysis.risk import MarketHealthReport
from ..config import PROJECT_ROOT

LOG_REL_PATH = Path("data") / "risk_score_history.jsonl"


def append_risk_score_log(health: MarketHealthReport) -> Path | None:
    """Append one JSON line with score, uncapped score, and level. Returns path or None on failure."""
    path = PROJECT_ROOT / LOG_REL_PATH
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "score": health.score,
            "score_uncapped": getattr(health, "score_uncapped", health.score),
            "overall_risk": health.overall_risk,
            "critical_count": health.critical_count,
            "warning_count": health.warning_count,
            "leading_signal_count": health.leading_signal_count,
            "source": "github_actions"
            if os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
            else "local",
            "github_sha": os.environ.get("GITHUB_SHA"),
            "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        }
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
        return path
    except OSError:
        return None
