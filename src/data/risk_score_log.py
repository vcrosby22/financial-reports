"""Append-only JSONL log of risk scores per report run (local + CI).

File: ``data/risk_score_history.jsonl`` under the project root (``financial-agent/``).
In CI (``financial-reports`` repo), this file is committed back to ``main`` so
history accumulates across runs and the report can show trend indicators.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..analysis.risk import MarketHealthReport
from ..config import PROJECT_ROOT

LOG_REL_PATH = Path("data") / "risk_score_history.jsonl"


@dataclass
class RiskTrend:
    """Computed deltas for the trend indicator. None means not enough data."""
    current_uncapped: int
    prev_1d_uncapped: int | None = None
    prev_1w_uncapped: int | None = None
    prev_1m_uncapped: int | None = None
    prev_1d_level: str | None = None
    prev_1w_level: str | None = None
    prev_1m_level: str | None = None

    @property
    def delta_1d(self) -> int | None:
        return (self.current_uncapped - self.prev_1d_uncapped) if self.prev_1d_uncapped is not None else None

    @property
    def delta_1w(self) -> int | None:
        return (self.current_uncapped - self.prev_1w_uncapped) if self.prev_1w_uncapped is not None else None

    @property
    def delta_1m(self) -> int | None:
        return (self.current_uncapped - self.prev_1m_uncapped) if self.prev_1m_uncapped is not None else None

    @property
    def has_any(self) -> bool:
        return any(d is not None for d in [self.delta_1d, self.delta_1w, self.delta_1m])


def read_risk_score_history() -> list[dict]:
    """Load all records from the JSONL log. Returns [] on missing file or error."""
    path = PROJECT_ROOT / LOG_REL_PATH
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    except (OSError, json.JSONDecodeError):
        return records
    return records


def _find_nearest_record(
    records: list[dict], target_dt: datetime, max_drift_hours: int = 14,
) -> dict | None:
    """Find the record closest to *target_dt* within *max_drift_hours*.

    GitHub Actions cron can drift ~1-2 hours; we allow generous window.
    """
    best: dict | None = None
    best_delta = timedelta(hours=max_drift_hours)
    for rec in records:
        try:
            ts = datetime.fromisoformat(rec["ts_utc"])
        except (KeyError, ValueError):
            continue
        delta = abs(ts - target_dt)
        if delta < best_delta:
            best_delta = delta
            best = rec
    return best


def compute_trend(health: MarketHealthReport) -> RiskTrend:
    """Build a RiskTrend from the current score and historical JSONL data."""
    now_utc = datetime.now(timezone.utc)
    records = read_risk_score_history()
    trend = RiskTrend(current_uncapped=getattr(health, "score_uncapped", health.score))

    if not records:
        return trend

    day_ago = _find_nearest_record(records, now_utc - timedelta(days=1))
    week_ago = _find_nearest_record(records, now_utc - timedelta(days=7))
    month_ago = _find_nearest_record(records, now_utc - timedelta(days=30))

    if day_ago:
        trend.prev_1d_uncapped = day_ago.get("score_uncapped", day_ago.get("score"))
        trend.prev_1d_level = day_ago.get("overall_risk")
    if week_ago:
        trend.prev_1w_uncapped = week_ago.get("score_uncapped", week_ago.get("score"))
        trend.prev_1w_level = week_ago.get("overall_risk")
    if month_ago:
        trend.prev_1m_uncapped = month_ago.get("score_uncapped", month_ago.get("score"))
        trend.prev_1m_level = month_ago.get("overall_risk")

    return trend


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
