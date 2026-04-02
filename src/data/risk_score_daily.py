"""One canonical risk score snapshot per calendar day (US Eastern date).

File: ``data/risk_score_daily.json`` under ``financial-agent/``.

* **Day key:** ``YYYY-MM-DD`` in ``America/New_York`` (same calendar the HTML
  report uses for “today”).
* **Multiple runs:** the last successful write for that day overwrites earlier
  runs, so scheduled CI near market hours wins over ad-hoc local runs the same
  morning.
* **Purpose:** stable history for trends and a future UI chart; complements the
  append-only per-run ``risk_score_history.jsonl``.

Note: In this monorepo, ``financial-agent/data/`` may be gitignored; the public
``financial-reports`` workflow should commit this file (or an allowlist) if you
want history on GitHub Pages.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ..analysis.risk import MarketHealthReport
from ..config import PROJECT_ROOT

DAILY_REL_PATH = Path("data") / "risk_score_daily.json"
SNAPSHOT_TZ_NAME = "America/New_York"


def _snapshot_day_et(when_utc: datetime | None = None) -> date:
    """Calendar date in Eastern time for bucketing daily snapshots."""
    utc = when_utc or datetime.now(timezone.utc)
    if utc.tzinfo is None:
        utc = utc.replace(tzinfo=timezone.utc)
    return utc.astimezone(ZoneInfo(SNAPSHOT_TZ_NAME)).date()


def _record_from_health(health: MarketHealthReport, ts_utc: datetime) -> dict:
    return {
        "snapshot_date": _snapshot_day_et(ts_utc).isoformat(),
        "ts_utc": ts_utc.isoformat(),
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


def _default_store() -> dict:
    return {
        "version": 1,
        "snapshot_calendar": SNAPSHOT_TZ_NAME,
        "by_date": {},
    }


def load_daily_store(path: Path | None = None) -> dict:
    """Load the daily JSON store; return an empty structure if missing or invalid."""
    p = path if path is not None else PROJECT_ROOT / DAILY_REL_PATH
    if not p.exists():
        return _default_store()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_store()
    if not isinstance(raw, dict):
        return _default_store()
    by_date = raw.get("by_date")
    if not isinstance(by_date, dict):
        by_date = {}
    out = _default_store()
    out["by_date"] = {str(k): v for k, v in by_date.items() if isinstance(v, dict)}
    return out


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=False) + "\n"
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def upsert_daily_risk_snapshot(health: MarketHealthReport) -> Path | None:
    """Write or replace today's Eastern calendar entry. Returns path or None."""
    ts_utc = datetime.now(timezone.utc)
    day_key = _snapshot_day_et(ts_utc).isoformat()
    path = PROJECT_ROOT / DAILY_REL_PATH
    try:
        store = load_daily_store(path)
        store["by_date"][day_key] = _record_from_health(health, ts_utc)
        _atomic_write_json(path, store)
        return path
    except OSError:
        return None


def get_daily_snapshot_for_date(d: date, path: Path | None = None) -> dict | None:
    """Return the stored record for an Eastern calendar date, or None."""
    store = load_daily_store(path)
    rec = store["by_date"].get(d.isoformat())
    return rec if isinstance(rec, dict) else None


def list_daily_snapshots_chronological(path: Path | None = None) -> list[dict]:
    """All daily records sorted by ``snapshot_date`` ascending (for future UI)."""
    store = load_daily_store(path)
    rows: list[dict] = []
    for _k, rec in store["by_date"].items():
        if isinstance(rec, dict) and rec.get("snapshot_date"):
            rows.append(rec)
    rows.sort(key=lambda r: r.get("snapshot_date", ""))
    return rows
