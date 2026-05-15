"""Supply chain cascade engine v2 — scoring, convergence, dual-rule, confidence, timeline tests."""

import json
import tempfile
from datetime import date, timedelta
from unittest.mock import patch

from src.analysis.supply_chain import (
    CascadeStage,
    CONVERGENCE_BOOST,
    CONVERGENCE_MIN_SCORE,
    CONVERGENCE_THRESHOLD,
    STAGE_OFFSETS,
    STAGE_RELATIVE_LABELS,
    _compute_stage_dates,
    _load_stage_transitions,
    _resolve_crisis_start,
    evaluate_cascade,
)
from src.data.macro import MacroIndicator, MacroSnapshot


def _make_macro(*indicators: tuple[str, float, str]) -> MacroSnapshot:
    """Build a minimal MacroSnapshot from (series_id, value, signal) tuples."""
    snap = MacroSnapshot()
    for sid, val, sig in indicators:
        snap.indicators.append(
            MacroIndicator(series_id=sid, name=sid, value=val, signal=sig)
        )
    return snap


def _make_proxy(ticker: str, price: float = 100.0, change_1m: float = 0.0) -> dict:
    return {ticker: {"ticker": ticker, "price": price, "change_pct_1m": change_1m}}


# ── Basic structure ───────────────────────────────────────────────────────


def test_returns_six_stages():
    stages = evaluate_cascade({}, None)
    assert len(stages) == 6
    names = [s.name for s in stages]
    assert "Oil Price Shock" in names
    assert "Energy Cost Cascade" in names
    assert "Helium & Semiconductor Squeeze" in names
    assert "Fertilizer & Food Pressure" in names
    assert "Pharmaceutical Delays" in names
    assert "Industrial Slowdown" in names


def test_no_stages_7_8():
    stages = evaluate_cascade({}, None)
    names = [s.name for s in stages]
    assert "Infrastructure Rebuild" not in names
    assert "New Supply Equilibrium" not in names


def test_empty_data_all_not_started():
    stages = evaluate_cascade({}, None)
    for s in stages:
        assert s.status == "not_started"


# ── Confidence = data completeness ────────────────────────────────────────


def test_confidence_is_data_completeness_ratio():
    macro = _make_macro(("DCOILBRENTEU", 100, "warning"))
    commodities = [{"ticker": "BZ=F", "price": 95, "change_pct_1m": 12}]
    stages = evaluate_cascade({}, macro, commodities)
    stage1 = stages[0]
    assert stage1.inputs_expected > 0
    assert stage1.inputs_received <= stage1.inputs_expected
    assert abs(stage1.confidence - stage1.inputs_received / stage1.inputs_expected) < 0.01


def test_confidence_zero_when_no_inputs():
    stages = evaluate_cascade({}, None)
    for s in stages:
        if s.inputs_expected > 0 and s.inputs_received == 0:
            assert s.confidence == 0.0


# ── Dual-rule thresholds ─────────────────────────────────────────────────


def test_high_score_without_momentum_is_projected():
    """Score >= 0.5 but no momentum should be 'projected', not 'active'."""
    macro = _make_macro(
        ("DCOILBRENTEU", 90, "neutral"),
    )
    commodities = [
        {"ticker": "BZ=F", "price": 90, "change_pct_1m": 5},
        {"ticker": "CL=F", "price": 85, "change_pct_1m": 5},
    ]
    stages = evaluate_cascade({}, macro, commodities)
    stage1 = stages[0]
    if stage1.stress_score >= 0.5 and not stage1.has_momentum:
        assert stage1.status == "projected"


def test_high_score_with_momentum_is_active():
    """Score >= 0.5 AND momentum → active."""
    macro = _make_macro(
        ("DCOILBRENTEU", 100, "warning"),
    )
    commodities = [
        {"ticker": "BZ=F", "price": 100, "change_pct_1m": 15},
    ]
    stages = evaluate_cascade({}, macro, commodities)
    stage1 = stages[0]
    assert stage1.status == "active"
    assert stage1.has_momentum is True


# ── Convergence amplifier ─────────────────────────────────────────────────


def test_convergence_boosts_moderate_stages():
    """When 3+ stages have score >= 0.3, each gets a convergence boost."""
    macro = _make_macro(
        ("DCOILBRENTEU", 100, "critical"),
        ("GASREGW", 5.5, "critical"),
        ("INDPRO", 95, "critical"),
        ("CUSR0000SAF11", 300, "critical"),
        ("PPIACO", 250, "warning"),
        ("WPU06790303", 200, "warning"),
        ("TCU", 70, "critical"),
        ("MANEMP", 12000, "critical"),
        ("DGORDER", 250000, "warning"),
        ("UNRATE", 6.5, "critical"),
    )
    proxy_data = {
        **_make_proxy("NG=F", change_1m=20),
        **_make_proxy("SOXX", change_1m=-12),
        **_make_proxy("HG=F", change_1m=-10),
        **_make_proxy("ZW=F", change_1m=15),
        **_make_proxy("ZC=F", change_1m=12),
        **_make_proxy("DBA", change_1m=8),
        **_make_proxy("SLX", change_1m=-10),
    }
    commodities = [
        {"ticker": "BZ=F", "price": 130, "change_pct_1m": 20},
    ]

    stages = evaluate_cascade(proxy_data, macro, commodities)
    moderate = [s for s in stages if s.stress_score >= CONVERGENCE_MIN_SCORE]
    assert len(moderate) >= CONVERGENCE_THRESHOLD, (
        f"Expected at least {CONVERGENCE_THRESHOLD} moderate stages, got {len(moderate)}"
    )
    for s in moderate:
        assert any("Convergence amplifier" in e for e in s.evidence)


def test_no_convergence_below_threshold():
    """Fewer than 3 moderate stages → no convergence boost."""
    stages = evaluate_cascade({}, None)
    for s in stages:
        assert not any("Convergence amplifier" in e for e in s.evidence)


# ── Graceful fallback (optional APIs = None) ──────────────────────────────


def test_graceful_without_hormuz():
    stages = evaluate_cascade({}, None, hormuz=None)
    stage1 = stages[0]
    assert stage1.inputs_expected == 4


def test_graceful_without_fda():
    stages = evaluate_cascade({}, None, fda_shortages=None)
    assert len(stages) == 6


def test_graceful_without_eia():
    stages = evaluate_cascade({}, None, eia=None)
    stage2 = stages[1]
    assert stage2.inputs_expected == 3


# ── Stage-specific data sources ───────────────────────────────────────────


def test_stage1_hormuz_high_risk_activates():
    """Hormuz risk score >= 7 with momentum → Stage 1 active."""
    from src.data.hormuz import HormuzSnapshot

    hormuz = HormuzSnapshot(
        risk_score=8.5,
        risk_level="critical",
        reduction_pct=55,
        lane_status="restricted",
        war_risk_premium_pct=2.5,
    )
    macro = _make_macro(("DCOILBRENTEU", 110, "warning"))
    commodities = [{"ticker": "BZ=F", "price": 110, "change_pct_1m": 18}]
    stages = evaluate_cascade({}, macro, commodities, hormuz=hormuz)
    assert stages[0].status == "active"
    assert any("Hormuz" in e for e in stages[0].evidence)


def test_tankermap_traffic_is_stage1_evidence_only():
    """TankerMap grounds Stage 1 evidence without changing score/status yet."""
    from src.data.tankermap import HormuzTrafficSnapshot

    baseline = evaluate_cascade({}, None)[0]
    tankermap = HormuzTrafficSnapshot(
        status="ok",
        current_zone_vessels=1,
        current_7d_avg=0.4,
        current_7d_total=3,
        normal_daily_avg=21.0,
        percent_of_normal=0.019,
    )

    with_traffic = evaluate_cascade({}, None, tankermap_traffic=tankermap)[0]

    assert with_traffic.status == baseline.status
    assert with_traffic.stress_score == baseline.stress_score
    assert with_traffic.inputs_expected == baseline.inputs_expected
    assert any("TankerMap traffic" in e for e in with_traffic.evidence)
    assert any("exact-zone vessels" in e for e in with_traffic.evidence)


def test_stage4_nitrogen_ppi_feeds_score():
    """Nitrogen PPI warning should contribute to Stage 4 score."""
    macro = _make_macro(
        ("WPU06790303", 200, "warning"),
        ("CUSR0000SAF11", 300, "warning"),
    )
    stages = evaluate_cascade({}, macro)
    stage4 = stages[3]
    assert stage4.stress_score > 0
    assert any("Nitrogen" in e or "nitrogen" in e for e in stage4.evidence)


def test_stage5_fda_shortages_feeds_score():
    from src.data.openfda import FDAShortageSnapshot

    fda = FDAShortageSnapshot(total_active=200, new_last_30d=60, new_last_90d=150)
    stages = evaluate_cascade({}, None, fda_shortages=fda)
    stage5 = stages[4]
    assert stage5.stress_score > 0
    assert any("openFDA" in e for e in stage5.evidence)


def test_stage6_manufacturing_composite():
    """Stage 6 uses INDPRO + TCU + MANEMP + DGORDER composite."""
    macro = _make_macro(
        ("INDPRO", 95, "warning"),
        ("TCU", 71, "critical"),
        ("MANEMP", 12000, "warning"),
        ("DGORDER", 250000, "warning"),
        ("UNRATE", 5.5, "warning"),
    )
    proxy = {**_make_proxy("SLX", change_1m=-10)}
    stages = evaluate_cascade(proxy, macro)
    stage6 = stages[5]
    assert stage6.stress_score > 0.5
    assert any("Capacity utilization" in e for e in stage6.evidence)
    assert any("Manufacturing employment" in e for e in stage6.evidence)


# ── Helium baseline ───────────────────────────────────────────────────────


def test_helium_baseline_loads():
    """Stage 3 should include helium context from USGS baseline."""
    stages = evaluate_cascade({}, None)
    stage3 = stages[2]
    has_helium = any("Helium" in e or "helium" in e or "Qatar" in e for e in stage3.evidence)
    assert has_helium, f"Expected helium context in Stage 3 evidence: {stage3.evidence}"


# ── Dynamic timeline tests ────────────────────────────────────────────────


def test_stage_offsets_count_matches_stages():
    assert len(STAGE_OFFSETS) == 6
    assert len(STAGE_RELATIVE_LABELS) == 6


def test_resolve_crisis_start_hormuz_priority():
    """Hormuz crisis_active + duration_days takes priority over config."""
    from src.data.hormuz import HormuzSnapshot

    hormuz = HormuzSnapshot(crisis_active=True, duration_days=30)
    config = {"supply_chain": {"crisis_start_override": "2026-01-01"}}
    result = _resolve_crisis_start(hormuz, config)
    expected = date.today() - timedelta(days=30)
    assert result == expected


def test_resolve_crisis_start_config_override():
    """Config override used when Hormuz has no active crisis."""
    from src.data.hormuz import HormuzSnapshot

    hormuz = HormuzSnapshot(crisis_active=False)
    config = {"supply_chain": {"crisis_start_override": "2026-01-15"}}
    result = _resolve_crisis_start(hormuz, config)
    assert result == date(2026, 1, 15)


def test_resolve_crisis_start_none_when_no_data():
    assert _resolve_crisis_start(None, None) is None
    assert _resolve_crisis_start(None, {}) is None
    assert _resolve_crisis_start(None, {"supply_chain": {}}) is None


def test_resolve_crisis_start_config_null():
    config = {"supply_chain": {"crisis_start_override": None}}
    assert _resolve_crisis_start(None, config) is None


def test_compute_stage_dates_populates_fields():
    stages = [
        CascadeStage(timeframe="placeholder", name=f"Stage {i+1}",
                      description="", status="not_started", confidence=0.0)
        for i in range(6)
    ]
    crisis_start = date(2026, 1, 15)
    _compute_stage_dates(crisis_start, stages)

    for i, s in enumerate(stages):
        start_off, end_off = STAGE_OFFSETS[i]
        assert s.date_range_start == crisis_start + timedelta(days=start_off)
        assert s.date_range_end == crisis_start + timedelta(days=end_off)
        assert "–" in s.timeframe or "-" in s.timeframe


def test_compute_stage_dates_model_should_be_active():
    """model_should_be_active is True when today falls within stage window."""
    today = date.today()
    crisis_start = today - timedelta(days=7)
    stages = [
        CascadeStage(timeframe="", name="Oil Price Shock",
                      description="", status="not_started", confidence=0.0)
    ]
    _compute_stage_dates(crisis_start, stages)
    assert stages[0].model_should_be_active is True


def test_compute_stage_dates_model_not_active_future():
    """model_should_be_active is False when stage window is in the future."""
    today = date.today()
    crisis_start = today
    stages = [
        CascadeStage(timeframe="", name="Stage 6",
                      description="", status="not_started", confidence=0.0)
        for _ in range(6)
    ]
    _compute_stage_dates(crisis_start, stages)
    assert stages[5].model_should_be_active is False


def test_no_crisis_uses_relative_labels():
    """When no crisis start is resolved, stages get relative timeframe labels."""
    stages = evaluate_cascade({}, None)
    for i, s in enumerate(stages):
        assert "from disruption" in s.timeframe, (
            f"Stage {i} timeframe should contain 'from disruption': {s.timeframe}"
        )


def test_config_override_anchors_dates():
    """crisis_start_override in config triggers date range computation."""
    config = {"supply_chain": {"crisis_start_override": "2026-01-15"}}
    stages = evaluate_cascade({}, None, config=config)
    assert stages[0].date_range_start == date(2026, 1, 15)
    assert stages[0].date_range_end == date(2026, 1, 29)
    assert "Jan" in stages[0].timeframe


def test_load_stage_transitions_parses_jsonl(tmp_path):
    """_load_stage_transitions parses JSONL for earliest activation dates."""
    jsonl = tmp_path / "history.jsonl"
    records = [
        {"date": "2026-03-01", "stages": [
            {"name": "Oil Price Shock", "status": "active"},
            {"name": "Energy Cost Cascade", "status": "projected"},
        ]},
        {"date": "2026-03-05", "stages": [
            {"name": "Oil Price Shock", "status": "active"},
            {"name": "Energy Cost Cascade", "status": "active"},
        ]},
        {"date": "2026-03-10", "stages": [
            {"name": "Oil Price Shock", "status": "active"},
            {"name": "Energy Cost Cascade", "status": "active"},
        ]},
    ]
    with open(jsonl, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    with patch("src.analysis.supply_chain.HISTORY_PATH", jsonl):
        transitions = _load_stage_transitions()

    assert transitions["Oil Price Shock"] == date(2026, 3, 1)
    assert transitions["Energy Cost Cascade"] == date(2026, 3, 5)
    assert "Fertilizer & Food Pressure" not in transitions


def test_load_stage_transitions_empty_file(tmp_path):
    jsonl = tmp_path / "empty.jsonl"
    jsonl.write_text("")
    with patch("src.analysis.supply_chain.HISTORY_PATH", jsonl):
        transitions = _load_stage_transitions()
    assert transitions == {}


def test_cascade_stage_new_fields_default():
    """New timeline fields default to None/False."""
    s = CascadeStage(timeframe="", name="", description="",
                     status="not_started", confidence=0.0)
    assert s.date_range_start is None
    assert s.date_range_end is None
    assert s.model_should_be_active is False
    assert s.first_activated_date is None


def test_active_stage_triggers_historical_fallback():
    """When a stage is active and no Hormuz/config, _2026_PEAK_DATE is used."""
    macro = _make_macro(("DCOILBRENTEU", 100, "warning"))
    commodities = [{"ticker": "BZ=F", "price": 100, "change_pct_1m": 15}]
    stages = evaluate_cascade({}, macro, commodities)
    if any(s.status == "active" for s in stages):
        assert stages[0].date_range_start is not None, (
            "Active stage should trigger historical fallback for date anchoring"
        )
