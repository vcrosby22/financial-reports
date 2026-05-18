"""Unit tests for historical analog decline aggregation."""

from __future__ import annotations

from src.analysis.projection import compute_bottom_estimate
from src.personal.historical import CRASHES


def test_compute_bottom_estimate_uses_explicit_peak_for_drawdown_and_levels():
    oil = next(c for c in CRASHES if c.name == "1973-1974 Oil Crisis")
    covid = next(c for c in CRASHES if c.name == "2020 COVID-19 Crash")
    # Broad enough to match both analogs (overlap weighting stays non-zero).
    factors = frozenset({"commodity_shock", "geopolitical", "stagflation", "external_shock"})

    result = compute_bottom_estimate(
        6500.0,
        [oil, covid],
        factors,
        peak=7000.0,
    )

    assert result is not None
    assert result.peak_level == 7000.0
    assert result.current_level == 6500.0
    expected_dd = round((6500.0 - 7000.0) / 7000.0 * 100, 1)
    assert result.current_decline_pct == expected_dd
    assert result.base_decline < 0
    assert set(result.analogs_used) == {"1973-1974 Oil Crisis", "2020 COVID-19 Crash"}
