"""Cross-source validity check tests.

Verifies that the actual Hormuz drift discovered on 2026-05-13
(API claimed brent_usd $122.40 vs. yfinance $106) flags as a
warn-level drift, not as silently green. This is the test that
encodes BL-110 against future regression.
"""

from __future__ import annotations

from src.analysis.cross_source import (
    CrossSourceCheck,
    DRIFT_FAIL_PCT,
    DRIFT_WARN_PCT,
    overall_validity,
    per_source_validity,
    to_log_record,
    validate,
)


class _Hormuz:
    def __init__(self, brent_usd=None, wti_usd=None):
        self.brent_usd = brent_usd
        self.wti_usd = wti_usd


def _md(brent=None, wti=None) -> dict:
    commodities = []
    if brent is not None:
        commodities.append({"ticker": "BZ=F", "price": brent})
    if wti is not None:
        commodities.append({"ticker": "CL=F", "price": wti})
    return {"commodities": commodities}


def test_aligned_values_are_ok():
    checks = validate({"hormuz": _Hormuz(brent_usd=106.5, wti_usd=103.2), "market_data": _md(brent=106.0, wti=103.0)})
    assert overall_validity(checks) == "ok"
    assert all(c.status == "ok" for c in checks)
    # Drift should be small and signed.
    brent = checks[0]
    assert brent.drift_pct is not None and abs(brent.drift_pct) < DRIFT_WARN_PCT


def test_hormuz_brent_inflation_flags_warn():
    """The actual 2026-05-13 numbers — Hormuz says $122.40, yfinance says $106."""
    checks = validate({"hormuz": _Hormuz(brent_usd=122.40, wti_usd=116.80), "market_data": _md(brent=106.13, wti=103.45)})
    overall = overall_validity(checks)
    assert overall in ("drift_warn", "drift_fail"), f"Expected warn/fail, got {overall}"
    brent = next(c for c in checks if c.label.startswith("Hormuz Brent"))
    assert brent.status == "drift_warn"
    assert brent.drift_pct is not None and brent.drift_pct > DRIFT_WARN_PCT


def test_extreme_drift_flags_fail():
    """A 50% over-claim should hit the fail threshold, not just warn."""
    checks = validate({"hormuz": _Hormuz(brent_usd=160.0), "market_data": _md(brent=100.0)})
    brent = next(c for c in checks if c.label.startswith("Hormuz Brent"))
    assert brent.status == "drift_fail"
    assert brent.drift_pct is not None and brent.drift_pct >= DRIFT_FAIL_PCT


def test_missing_ground_truth_is_unavailable_not_pass():
    """If yfinance Brent isn't present, we can't validate — must surface, not silently pass."""
    checks = validate({"hormuz": _Hormuz(brent_usd=122.40), "market_data": _md(brent=None)})
    brent = next(c for c in checks if c.label.startswith("Hormuz Brent"))
    assert brent.status == "unavailable"
    assert brent.drift_pct is None


def test_missing_source_under_test_is_unavailable():
    """If the integration didn't return a value, we can't validate either."""
    checks = validate({"hormuz": _Hormuz(brent_usd=None), "market_data": _md(brent=106.0)})
    brent = next(c for c in checks if c.label.startswith("Hormuz Brent"))
    assert brent.status == "unavailable"


def test_per_source_validity_returns_worst_status():
    """If Brent is warn and WTI is fail, the per-source rollup should be fail."""
    checks = validate({"hormuz": _Hormuz(brent_usd=122.40, wti_usd=160.0), "market_data": _md(brent=106.0, wti=100.0)})
    rollup = per_source_validity(checks)
    assert rollup["hormuz"] == "drift_fail"


def test_overall_validity_no_checks():
    """Empty input shouldn't raise — should return 'unavailable'."""
    assert overall_validity([]) == "unavailable"


def test_log_record_serializes_safely():
    checks = validate({"hormuz": _Hormuz(brent_usd=122.40), "market_data": _md(brent=106.0)})
    rec = to_log_record(checks)
    assert "run_at" in rec
    assert rec["overall"] in ("ok", "drift_warn", "drift_fail", "unavailable")
    assert isinstance(rec["checks"], list)
    assert all("label" in c and "status" in c for c in rec["checks"])
