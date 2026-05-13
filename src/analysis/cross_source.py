"""Cross-source data validity checks.

Required by `external-data-source-integrity` rule item 6: any external
claim that overlaps with a known second source must be cross-validated
at build time, with drift surfaced in the artifact.

This module is the in-process enforcement layer. It runs every CI build,
compares known-overlapping fields between independent data sources
(e.g. Hormuz Monitor's `brent_usd` vs. yfinance `BZ=F` close), and
returns a list of `CrossSourceCheck` records the report uses to color
the data-source footer dots and to optionally surface a drift sub-line.

The companion on-demand version lives in
`.cursor/skills/data-source-health-audit/SKILL.md` (cross-validation
pass section); both share this module so the rule stays one source of
truth.

History:
- 2026-05-13 — discovered Hormuz Monitor's `brent_usd 122.40` is
  ~14% inflated vs. independent ground truth (yfinance + TradingEconomics
  agree on ~$106). Added BL-110, then this module as the fix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

# Drift thresholds — calibrate as we observe real values over time.
# Below WARN: green (live and aligned).
# Between WARN and FAIL: yellow (live but drifting; reader should be cautious).
# Above FAIL: red (live but the value is unreliable; treat as broken).
DRIFT_WARN_PCT = 10.0
DRIFT_FAIL_PCT = 20.0


@dataclass
class CrossSourceCheck:
    """One overlap check between two independent sources.

    Fields:
      label: human-readable name of the check, e.g. "Hormuz Brent vs yfinance BZ=F"
      source_a, value_a: the source under test, and its claimed value
      source_b, value_b: the ground-truth comparison source, and its value
      drift_pct: |value_a - value_b| / value_b * 100, signed (+ = a higher than b)
      status: "ok" | "drift_warn" | "drift_fail" | "unavailable"
      reason: human-readable detail when status != "ok"
    """

    label: str
    source_a: str
    value_a: float | None
    source_b: str
    value_b: float | None
    drift_pct: float | None = None
    status: str = "unavailable"
    reason: str = ""

    @property
    def severity(self) -> int:
        return {"ok": 0, "drift_warn": 1, "drift_fail": 2, "unavailable": 1}.get(self.status, 1)


def _drift_status(value_a: float | None, value_b: float | None) -> tuple[str, float | None, str]:
    """Compute drift status between two scalar values.

    Returns (status, drift_pct, reason). drift_pct is signed.
    """
    if value_a is None or value_b is None:
        if value_a is None and value_b is None:
            return "unavailable", None, "Both sources missing"
        missing = "source A" if value_a is None else "source B"
        return "unavailable", None, f"{missing} missing"
    if value_b == 0:
        return "unavailable", None, "Cannot divide by zero ground truth"
    drift = (value_a - value_b) / abs(value_b) * 100.0
    abs_drift = abs(drift)
    if abs_drift >= DRIFT_FAIL_PCT:
        return (
            "drift_fail",
            drift,
            f"Drift {drift:+.1f}% exceeds fail threshold {DRIFT_FAIL_PCT:.0f}%",
        )
    if abs_drift >= DRIFT_WARN_PCT:
        return (
            "drift_warn",
            drift,
            f"Drift {drift:+.1f}% exceeds warn threshold {DRIFT_WARN_PCT:.0f}%",
        )
    return "ok", drift, ""


# ---------------------------------------------------------------------------
# Overlap matrix
# ---------------------------------------------------------------------------
# Each entry is a callable that, given the named-snapshots dict, returns a
# CrossSourceCheck. Adding a new overlap = adding a new entry here. Sources
# without an obvious second source (e.g. Hormuz LNG JKM) intentionally have
# no entry — the rule allows "no overlap available, document why" but we
# track those absences in the audit skill, not silently.
# ---------------------------------------------------------------------------


def _yfinance_price(market_data: dict | None, ticker: str) -> float | None:
    if not market_data:
        return None
    for bucket in ("commodities", "indices", "etfs", "stocks", "crypto"):
        for row in market_data.get(bucket, []) or []:
            if row.get("ticker") == ticker and row.get("price"):
                return float(row["price"])
    return None


def _hormuz_brent_check(snapshots: dict[str, Any]) -> CrossSourceCheck:
    hormuz = snapshots.get("hormuz")
    market_data = snapshots.get("market_data")
    a = getattr(hormuz, "brent_usd", None) if hormuz else None
    b = _yfinance_price(market_data, "BZ=F")
    status, drift, reason = _drift_status(a, b)
    return CrossSourceCheck(
        label="Hormuz Brent vs yfinance BZ=F",
        source_a="hormuz.brent_usd",
        value_a=a,
        source_b="yfinance.BZ=F",
        value_b=b,
        drift_pct=drift,
        status=status,
        reason=reason,
    )


def _hormuz_wti_check(snapshots: dict[str, Any]) -> CrossSourceCheck:
    hormuz = snapshots.get("hormuz")
    market_data = snapshots.get("market_data")
    a = getattr(hormuz, "wti_usd", None) if hormuz else None
    b = _yfinance_price(market_data, "CL=F")
    status, drift, reason = _drift_status(a, b)
    return CrossSourceCheck(
        label="Hormuz WTI vs yfinance CL=F",
        source_a="hormuz.wti_usd",
        value_a=a,
        source_b="yfinance.CL=F",
        value_b=b,
        drift_pct=drift,
        status=status,
        reason=reason,
    )


# Registered checks. Order matters only for human-readable output.
OVERLAP_MATRIX: list[Callable[[dict[str, Any]], CrossSourceCheck]] = [
    _hormuz_brent_check,
    _hormuz_wti_check,
]


def validate(snapshots: dict[str, Any]) -> list[CrossSourceCheck]:
    """Run every registered cross-source check against the provided snapshots.

    `snapshots` is a free-form dict with well-known keys the registered
    checks understand. Current readers expect:
      - "hormuz": HormuzSnapshot | None
      - "market_data": dict (from yfinance fetch) | None
      - "macro_data": MacroSnapshot | None  (room for future FRED checks)
      - "fda": FDAShortageSnapshot | None
      - "eia": EIASnapshot | None

    Checks for sources not present in `snapshots` return status="unavailable".
    """
    return [check(snapshots) for check in OVERLAP_MATRIX]


# ---------------------------------------------------------------------------
# Aggregation helpers used by the report and the audit skill.
# ---------------------------------------------------------------------------


def overall_validity(checks: list[CrossSourceCheck]) -> str:
    """Reduce a list of checks to a single status.

    Worst-status-wins: any drift_fail -> drift_fail; any drift_warn -> drift_warn;
    no checks at all -> unavailable; everything ok -> ok.
    """
    if not checks:
        return "unavailable"
    by_severity = sorted(checks, key=lambda c: c.severity, reverse=True)
    return by_severity[0].status


def per_source_validity(checks: list[CrossSourceCheck]) -> dict[str, str]:
    """Map each source-under-test to its worst observed status across checks.

    The source-under-test is the prefix before the first dot in `source_a`,
    e.g. "hormuz.brent_usd" -> "hormuz". Used by the report footer to color
    each integration's dot by validity, not just presence.
    """
    out: dict[str, str] = {}
    for c in checks:
        src = c.source_a.split(".", 1)[0]
        cur = out.get(src, "ok")
        if {"ok": 0, "drift_warn": 1, "unavailable": 1, "drift_fail": 2}.get(c.status, 1) > {
            "ok": 0,
            "drift_warn": 1,
            "unavailable": 1,
            "drift_fail": 2,
        }.get(cur, 0):
            out[src] = c.status
    return out


def to_log_record(checks: list[CrossSourceCheck]) -> dict[str, Any]:
    """Serialize a run's worth of checks for `data/validation_log.jsonl`."""
    return {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "overall": overall_validity(checks),
        "checks": [
            {
                "label": c.label,
                "source_a": c.source_a,
                "value_a": c.value_a,
                "source_b": c.source_b,
                "value_b": c.value_b,
                "drift_pct": round(c.drift_pct, 2) if c.drift_pct is not None else None,
                "status": c.status,
                "reason": c.reason,
            }
            for c in checks
        ],
    }
