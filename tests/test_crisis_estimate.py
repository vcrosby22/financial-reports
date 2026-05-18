"""Crisis analog range safety tests."""

from __future__ import annotations

from types import SimpleNamespace

from src.report import (
    _analog_range_unavailable_html,
    _bottom_estimate_range_bar,
    _crisis_estimate_gate,
    _sp500_high_water_mark,
)


def test_sp500_high_water_mark_prefers_fetched_peak_over_stale_constant():
    peak, source = _sp500_high_water_mark(
        {"ticker": "^GSPC", "price": 7438.0, "fifty_two_week_high": 7520.0}
    )

    assert peak == 7520.0
    assert source == "52-week high"


def test_crisis_estimate_gate_hides_range_when_market_at_high():
    can_show, reasons = _crisis_estimate_gate(
        sp500_price=7438.0,
        peak_level=7438.0,
        macro_data=SimpleNamespace(indicators=[object()]),
        cascade_stages=[object()],
        cross_checks=[],
    )

    assert can_show is False
    assert any("high-water mark" in reason for reason in reasons)


def test_crisis_estimate_gate_hides_range_when_macro_missing():
    can_show, reasons = _crisis_estimate_gate(
        sp500_price=7000.0,
        peak_level=7500.0,
        macro_data=None,
        cascade_stages=[object()],
        cross_checks=[],
    )

    assert can_show is False
    assert any("macro inputs missing" in reason for reason in reasons)


def test_analog_range_copy_is_context_not_bottom_forecast():
    estimate = SimpleNamespace(
        optimistic_decline=-33.9,
        base_decline=-47.0,
        pessimistic_decline=-49.1,
        current_decline_pct=-1.1,
        base_level=3657,
        optimistic_days=33,
        base_days=600,
        pessimistic_days=929,
        confidence=0.6,
    )

    html = _bottom_estimate_range_bar(estimate, "1973-1974 Oil Crisis")

    assert "Historical Analog Range" in html
    assert "Analog midpoint" in html
    assert "quality gates fail" in html.lower()
    assert "This is not a live bottom forecast" in html
    assert "2026 Bottom Estimate" not in html
    assert "Base case" not in html
    assert "bottoming in" not in html


def test_unavailable_copy_explains_hidden_range():
    html = _analog_range_unavailable_html(["macro inputs missing"])

    assert "Historical Analog Range paused" in html
    assert "macro inputs missing" in html
    assert "live bottom estimate" in html


def test_crisis_estimate_gate_hides_range_on_drift_fail():
    chk = SimpleNamespace(status="drift_fail", label="test", drift_pct=-12.0, reason="n/a")
    can_show, reasons = _crisis_estimate_gate(
        sp500_price=7000.0,
        peak_level=7500.0,
        macro_data=SimpleNamespace(indicators=[object()]),
        cascade_stages=[object()],
        cross_checks=[chk],
    )

    assert can_show is False
    assert any("drift-critical" in r for r in reasons)
