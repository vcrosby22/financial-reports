"""Append-only JSONL log of risk scores per report run (local + CI).

File: ``data/risk_score_history.jsonl`` under the project root (``financial-agent/``).
In CI (``financial-reports`` repo), this file is committed back to ``main`` so
history accumulates across runs and the report can show trend indicators.

**Daily canonical snapshots** (one row per Eastern calendar day) live in
``data/risk_score_daily.json`` — see ``risk_score_daily.py``. :func:`compute_trend`
prefers that file for 1d / 1w / 1m baselines and falls back to this JSONL.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ..analysis.risk import MarketHealthReport
from ..config import PROJECT_ROOT

from .risk_score_daily import get_daily_snapshot_for_date

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


def _apply_baseline_from_record(trend: RiskTrend, rec: dict, horizon: str) -> None:
    uncapped = rec.get("score_uncapped", rec.get("score"))
    level = rec.get("overall_risk")
    if horizon == "1d":
        trend.prev_1d_uncapped = uncapped
        trend.prev_1d_level = level
    elif horizon == "1w":
        trend.prev_1w_uncapped = uncapped
        trend.prev_1w_level = level
    elif horizon == "1m":
        trend.prev_1m_uncapped = uncapped
        trend.prev_1m_level = level


def _find_recent_daily_snapshot(
    anchor: "date", max_lookback: int = 3,
) -> dict | None:
    """Walk backwards from *anchor* up to *max_lookback* days to find the most
    recent daily snapshot.  Bridges weekends and short holidays where CI doesn't
    run (Mon-Fri only)."""
    from datetime import date as _date  # local to avoid top-level circular
    for offset in range(max_lookback + 1):
        rec = get_daily_snapshot_for_date(anchor - timedelta(days=offset))
        if rec is not None:
            return rec
    return None


def compute_trend(health: MarketHealthReport) -> RiskTrend:
    """Build a RiskTrend from daily snapshots when possible, else per-run JSONL.

    For each horizon (1d / 1w / 1m) the daily JSON store is tried first,
    walking back up to 3 extra calendar days to bridge weekends and holidays.
    If no daily snapshot is found, the append-only JSONL is used with a generous
    drift window (72 h for 1d to survive 3-day weekends, 36 h for 1w/1m).
    """
    now_utc = datetime.now(timezone.utc)
    today_et = now_utc.astimezone(ZoneInfo("America/New_York")).date()
    records = read_risk_score_history()
    trend = RiskTrend(current_uncapped=getattr(health, "score_uncapped", health.score))

    d_daily = _find_recent_daily_snapshot(today_et - timedelta(days=1))
    w_daily = _find_recent_daily_snapshot(today_et - timedelta(days=7))
    m_daily = _find_recent_daily_snapshot(today_et - timedelta(days=30))

    if d_daily:
        _apply_baseline_from_record(trend, d_daily, "1d")
    elif records:
        day_ago = _find_nearest_record(records, now_utc - timedelta(days=1), max_drift_hours=72)
        if day_ago:
            _apply_baseline_from_record(trend, day_ago, "1d")

    if w_daily:
        _apply_baseline_from_record(trend, w_daily, "1w")
    elif records:
        week_ago = _find_nearest_record(records, now_utc - timedelta(days=7), max_drift_hours=36)
        if week_ago:
            _apply_baseline_from_record(trend, week_ago, "1w")

    if m_daily:
        _apply_baseline_from_record(trend, m_daily, "1m")
    elif records:
        month_ago = _find_nearest_record(records, now_utc - timedelta(days=30), max_drift_hours=36)
        if month_ago:
            _apply_baseline_from_record(trend, month_ago, "1m")

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
