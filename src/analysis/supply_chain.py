"""Dynamic supply chain cascade engine — v2.

Evaluates 6 cascade stages against live market data (yfinance proxies),
FRED macro indicators, and optional API sources (Hormuz Monitor, openFDA, EIA).

v2 improvements over v1:
- Convergence amplifier: 3+ moderate stages boost each other (real cascades compound)
- Dual-rule thresholds: activation requires both score level AND momentum confirmation
- Confidence = data completeness: inputs_received / inputs_expected per stage
- New data sources: nitrogen PPI, manufacturing composite, pharma ETF, INDA, Hormuz API
- Removed stages 7-8 (Infrastructure Rebuild / New Supply Equilibrium — too far out)
- Dynamic timelines: anchored date ranges when crisis start is known
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from ..data.macro import MacroSnapshot

HISTORY_PATH = Path(__file__).parent.parent.parent / "data" / "supply_chain_history.jsonl"
HELIUM_BASELINE_PATH = Path(__file__).parent.parent.parent / "data" / "helium_usgs_baseline.json"

CONVERGENCE_THRESHOLD = 3
CONVERGENCE_BOOST = 0.12
CONVERGENCE_MIN_SCORE = 0.3

# (start_days, end_days) relative to crisis onset for each stage index.
# Derived from historical supply-chain disruption propagation patterns.
STAGE_OFFSETS: list[tuple[int, int]] = [
    (0, 14),      # Stage 1: Oil Price Shock — Week 1-2
    (14, 60),     # Stage 2: Energy Cost Cascade — Month 1-2
    (45, 120),    # Stage 3: Helium & Semiconductor Squeeze — Month 2-4
    (75, 180),    # Stage 4: Fertilizer & Food Pressure — Month 3-6
    (105, 240),   # Stage 5: Pharmaceutical Delays — Month 4-8
    (165, 365),   # Stage 6: Industrial Slowdown — Month 6-12
]

# Relative labels (used when no crisis start is known)
STAGE_RELATIVE_LABELS: list[str] = [
    "Week 1-2 from disruption",
    "Month 1-2 from disruption",
    "Month 2-4 from disruption",
    "Month 3-6 from disruption",
    "Month 4-8 from disruption",
    "Month 6-12 from disruption",
]


@dataclass
class CascadeStage:
    timeframe: str
    name: str
    description: str
    status: str  # "active", "projected", "not_started"
    confidence: float  # data completeness: inputs_received / inputs_expected
    stress_score: float = 0.0  # raw accumulated score before gating
    inputs_expected: int = 0
    inputs_received: int = 0
    has_momentum: bool = False  # at least one input shows directional worsening
    evidence: list[str] = field(default_factory=list)
    date_range_start: date | None = None
    date_range_end: date | None = None
    model_should_be_active: bool = False
    first_activated_date: date | None = None


def _pct(data: dict | None, key: str = "change_pct_1m") -> float | None:
    if data is None:
        return None
    val = data.get(key)
    return float(val) if val is not None else None


def _price(data: dict | None) -> float | None:
    if data is None:
        return None
    val = data.get("price")
    return float(val) if val is not None else None


def _macro_val(macro: MacroSnapshot | None, series_id: str) -> float | None:
    if macro is None:
        return None
    for ind in macro.indicators:
        if ind.series_id == series_id:
            return ind.value
    return None


def _macro_signal(macro: MacroSnapshot | None, series_id: str) -> str | None:
    if macro is None:
        return None
    for ind in macro.indicators:
        if ind.series_id == series_id:
            return ind.signal
    return None


def _macro_yoy(macro: MacroSnapshot | None, series_id: str) -> float | None:
    if macro is None:
        return None
    for ind in macro.indicators:
        if ind.series_id == series_id:
            return getattr(ind, "yoy_change", None)
    return None


def _macro_change(macro: MacroSnapshot | None, series_id: str) -> float | None:
    if macro is None:
        return None
    for ind in macro.indicators:
        if ind.series_id == series_id:
            return getattr(ind, "change", None)
    return None


def _load_helium_baseline() -> dict | None:
    try:
        if HELIUM_BASELINE_PATH.exists():
            return json.loads(HELIUM_BASELINE_PATH.read_text())
    except Exception:
        pass
    return None


def _apply_dual_rule(stage: CascadeStage) -> None:
    """Apply dual-rule gating: status requires both score threshold AND momentum."""
    if stage.stress_score >= 0.5 and stage.has_momentum:
        stage.status = "active"
    elif stage.stress_score >= 0.5 and not stage.has_momentum:
        stage.status = "projected"
        stage.evidence.append("Score above threshold but no directional momentum confirmed")
    elif stage.stress_score >= 0.2:
        stage.status = "projected"
    else:
        stage.status = "not_started"


def _update_confidence(stage: CascadeStage) -> None:
    """Set confidence = data completeness ratio."""
    if stage.inputs_expected > 0:
        stage.confidence = stage.inputs_received / stage.inputs_expected
    else:
        stage.confidence = 0.0


def _resolve_crisis_start(hormuz=None, config: dict | None = None) -> date | None:
    """Determine crisis start date from best available source.

    Priority: (1) Hormuz API crisis_active + duration_days,
    (2) config.yaml crisis_start_override, (3) None.
    historical.py _2026_PEAK_DATE is used as fallback only when at least one
    stage is active (applied after scoring in evaluate_cascade).
    """
    if hormuz is not None and hormuz.crisis_active and hormuz.duration_days:
        return date.today() - timedelta(days=int(hormuz.duration_days))

    if config:
        override = config.get("supply_chain", {}).get("crisis_start_override")
        if override:
            if isinstance(override, date):
                return override
            try:
                return date.fromisoformat(str(override))
            except (ValueError, TypeError):
                pass

    return None


def _compute_stage_dates(
    crisis_start: date, stages: list[CascadeStage]
) -> None:
    """Populate date_range_start/end and model_should_be_active on each stage."""
    today = date.today()
    for i, stage in enumerate(stages):
        if i < len(STAGE_OFFSETS):
            start_off, end_off = STAGE_OFFSETS[i]
            stage.date_range_start = crisis_start + timedelta(days=start_off)
            stage.date_range_end = crisis_start + timedelta(days=end_off)
            stage.model_should_be_active = (
                stage.date_range_start <= today <= stage.date_range_end
            )
            stage.timeframe = (
                f"{stage.date_range_start.strftime('%b %-d')} – "
                f"{stage.date_range_end.strftime('%b %-d, %Y')}"
            )


def _load_stage_transitions() -> dict[str, date]:
    """Scan supply_chain_history.jsonl for the earliest date each stage became active."""
    transitions: dict[str, date] = {}
    try:
        if not HISTORY_PATH.exists():
            return transitions
        with open(HISTORY_PATH) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record_date_str = record.get("date")
                if not record_date_str:
                    continue
                try:
                    record_date = date.fromisoformat(record_date_str)
                except ValueError:
                    continue
                for stage_data in record.get("stages", []):
                    name = stage_data.get("name", "")
                    status = stage_data.get("status", "")
                    if status == "active" and name not in transitions:
                        transitions[name] = record_date
    except Exception:
        pass
    return transitions


def evaluate_cascade(
    proxy_data: dict[str, dict],
    macro: MacroSnapshot | None,
    commodities: list[dict] | None = None,
    hormuz=None,
    fda_shortages=None,
    eia=None,
    config: dict | None = None,
) -> list[CascadeStage]:
    """Evaluate all cascade stages and return live status.

    Args:
        proxy_data: dict mapping ticker to its fetch_ticker_data result.
        macro: current MacroSnapshot with FRED indicators.
        commodities: list of commodity dicts from the main report pipeline.
        hormuz: HormuzSnapshot from data/hormuz.py (optional).
        fda_shortages: FDAShortageSnapshot from data/openfda.py (optional).
        eia: EIASnapshot from data/eia.py (optional).
        config: parsed config.yaml dict (optional, for crisis_start_override).
    """
    commodity_map: dict[str, dict] = {}
    if commodities:
        for c in commodities:
            commodity_map[c.get("ticker", "")] = c

    stages: list[CascadeStage] = []

    # ── Stage 1: Oil Price Shock ──────────────────────────────────────────
    stage1 = CascadeStage(
        timeframe="Week 1-2",
        name="Oil Price Shock",
        description="Brent crude spikes, WTI follows. Gasoline prices at pump rise within days.",
        status="not_started",
        confidence=0.0,
        inputs_expected=6,  # brent_price, brent_signal, bz_1m, cl_1m, hormuz_risk, hormuz_traffic
    )
    brent_price = _macro_val(macro, "DCOILBRENTEU")
    brent_signal = _macro_signal(macro, "DCOILBRENTEU")
    bz = commodity_map.get("BZ=F")
    cl = commodity_map.get("CL=F")
    bz_1m = _pct(bz)
    cl_1m = _pct(cl)

    if brent_price is not None:
        stage1.inputs_received += 1
        if brent_price >= 85:
            stage1.stress_score += 0.4
            stage1.evidence.append(f"Brent at ${brent_price:.0f}")
            if brent_price >= 95:
                stage1.has_momentum = True
    if brent_signal is not None:
        stage1.inputs_received += 1
        if brent_signal in ("warning", "critical"):
            stage1.stress_score += 0.3
            stage1.has_momentum = True
            stage1.evidence.append(f"Brent FRED signal: {brent_signal}")
    if bz_1m is not None:
        stage1.inputs_received += 1
        if bz_1m > 10:
            stage1.stress_score += 0.3
            stage1.has_momentum = True
            stage1.evidence.append(f"Brent futures +{bz_1m:.1f}% (1m)")
    if cl_1m is not None:
        stage1.inputs_received += 1
        if cl_1m > 10:
            stage1.stress_score += 0.2
            stage1.has_momentum = True
            stage1.evidence.append(f"WTI futures +{cl_1m:.1f}% (1m)")

    if hormuz is not None:
        stage1.inputs_received += 1  # risk
        if hormuz.risk_score >= 7.0:
            stage1.stress_score += 0.5
            stage1.has_momentum = True
            stage1.evidence.append(
                f"Hormuz risk score {hormuz.risk_score:.1f}/10 ({hormuz.risk_level})"
            )
        elif hormuz.risk_score >= 5.0:
            stage1.stress_score += 0.2
            stage1.evidence.append(
                f"Hormuz risk elevated: {hormuz.risk_score:.1f}/10"
            )

        if hormuz.reduction_pct is not None:
            stage1.inputs_received += 1  # traffic
            if hormuz.reduction_pct > 50:
                stage1.stress_score += 0.4
                stage1.has_momentum = True
                stage1.evidence.append(
                    f"Strait traffic reduced {hormuz.reduction_pct:.0f}%"
                )
            elif hormuz.reduction_pct > 20:
                stage1.stress_score += 0.2
                stage1.evidence.append(
                    f"Strait traffic down {hormuz.reduction_pct:.0f}%"
                )
        else:
            stage1.inputs_expected -= 1  # traffic not available, adjust expected

        if hormuz.lane_status and hormuz.lane_status in ("restricted", "closed"):
            stage1.stress_score += 0.3
            stage1.has_momentum = True
            stage1.evidence.append(f"Strait lane status: {hormuz.lane_status}")

        if hormuz.war_risk_premium_pct is not None and hormuz.war_risk_premium_pct > 1.0:
            stage1.stress_score += 0.15
            stage1.evidence.append(
                f"War-risk insurance premium: {hormuz.war_risk_premium_pct:.1f}%"
            )
    else:
        stage1.inputs_expected = 4  # hormuz not available — only oil proxies

    _update_confidence(stage1)
    _apply_dual_rule(stage1)
    stages.append(stage1)

    # ── Stage 2: Energy Cost Cascade ──────────────────────────────────────
    stage2 = CascadeStage(
        timeframe="Month 1-2",
        name="Energy Cost Cascade",
        description="Natural gas prices spike (LNG rerouting). Electricity costs rise. Industrial production slows.",
        status="not_started",
        confidence=0.0,
        inputs_expected=5,  # ng_1m, gas_price, indpro, eia_storage, eia_futures
    )
    ng = proxy_data.get("NG=F")
    ng_1m = _pct(ng)
    gas_price = _macro_val(macro, "GASREGW")
    gas_signal = _macro_signal(macro, "GASREGW")
    indpro_signal = _macro_signal(macro, "INDPRO")

    if ng_1m is not None:
        stage2.inputs_received += 1
        if ng_1m > 15:
            stage2.stress_score += 0.4
            stage2.has_momentum = True
            stage2.evidence.append(f"Nat gas +{ng_1m:.1f}% (1m)")
        elif ng_1m > 5:
            stage2.stress_score += 0.2
            stage2.has_momentum = True
            stage2.evidence.append(f"Nat gas +{ng_1m:.1f}% (1m)")
    if gas_signal is not None:
        stage2.inputs_received += 1
        if gas_signal in ("warning", "critical"):
            stage2.stress_score += 0.3
            stage2.has_momentum = True
            stage2.evidence.append(f"Gasoline: ${gas_price:.2f}/gal ({gas_signal})")
    if indpro_signal is not None:
        stage2.inputs_received += 1
        if indpro_signal in ("warning", "critical"):
            stage2.stress_score += 0.3
            stage2.has_momentum = True
            stage2.evidence.append(f"Industrial production: {indpro_signal}")

    if eia is not None:
        if eia.ng_storage_bcf is not None:
            stage2.inputs_received += 1
            if eia.ng_storage_yoy_pct is not None and eia.ng_storage_yoy_pct < -15:
                stage2.stress_score += 0.25
                stage2.has_momentum = True
                stage2.evidence.append(
                    f"EIA: NG storage {eia.ng_storage_bcf:.0f} Bcf "
                    f"({eia.ng_storage_yoy_pct:+.1f}% vs prior)"
                )
        if eia.ng_futures_price is not None:
            stage2.inputs_received += 1
            if eia.ng_futures_price > 5.0:
                stage2.stress_score += 0.2
                stage2.evidence.append(
                    f"EIA: NG futures ${eia.ng_futures_price:.2f}/MMBtu (elevated)"
                )
    else:
        stage2.inputs_expected = 3

    _update_confidence(stage2)
    _apply_dual_rule(stage2)
    stages.append(stage2)

    # ── Stage 3: Helium & Semiconductor Squeeze ───────────────────────────
    stage3 = CascadeStage(
        timeframe="Month 2-4",
        name="Helium & Semiconductor Squeeze",
        description="Semiconductor fabs reduce output. Lead times extend. Helium spot prices surge.",
        status="not_started",
        confidence=0.0,
        inputs_expected=5,  # soxx, hg, dgorder, helium_baseline, upstream_active
    )
    soxx = proxy_data.get("SOXX")
    soxx_1m = _pct(soxx)
    hg = proxy_data.get("HG=F")
    hg_1m = _pct(hg)
    dgorder_signal = _macro_signal(macro, "DGORDER")
    dgorder_yoy = _macro_yoy(macro, "DGORDER")

    if soxx_1m is not None:
        stage3.inputs_received += 1
        if soxx_1m < -10:
            stage3.stress_score += 0.4
            stage3.has_momentum = True
            stage3.evidence.append(f"SOXX {soxx_1m:+.1f}% (1m) — chip sector under pressure")
        elif soxx_1m < -5:
            stage3.stress_score += 0.2
            stage3.has_momentum = True
            stage3.evidence.append(f"SOXX {soxx_1m:+.1f}% (1m)")
    if hg_1m is not None:
        stage3.inputs_received += 1
        if hg_1m < -8:
            stage3.stress_score += 0.2
            stage3.has_momentum = True
            stage3.evidence.append(f"Copper {hg_1m:+.1f}% (1m) — industrial demand weakening")
        elif abs(hg_1m) > 8:
            stage3.stress_score += 0.1
            stage3.evidence.append(f"Copper {hg_1m:+.1f}% (1m) — industrial demand signal")
    if dgorder_signal is not None:
        stage3.inputs_received += 1
        if dgorder_signal in ("warning", "critical"):
            stage3.stress_score += 0.25
            stage3.has_momentum = True
            stage3.evidence.append(
                f"Durable goods orders: {dgorder_signal}"
                + (f" ({dgorder_yoy:+.1f}% YoY)" if dgorder_yoy else "")
            )

    helium = _load_helium_baseline()
    if helium:
        stage3.inputs_received += 1
        qatar_share = helium.get("qatar_share_of_global_pct", 28)
        stage3.evidence.append(
            f"Helium context: Qatar supplies {qatar_share}% of global helium via Ras Laffan "
            f"(USGS {helium.get('year', '?')} data — ${helium.get('grade_a_price_per_mcf_usd', '?')}/Mcf)"
        )
        if stage1.status == "active":
            stage3.stress_score += 0.15
            stage3.evidence.append("Strait disruption threatens Ras Laffan helium exports")

    if stage2.status == "active":
        stage3.inputs_received += 1
        stage3.stress_score += 0.2
        stage3.has_momentum = True
        stage3.evidence.append("Energy cascade active → downstream pressure building")
    elif stage2.status == "projected":
        stage3.inputs_received += 1
        stage3.stress_score += 0.1
        stage3.evidence.append("Energy cascade projected → watch for downstream effects")

    _update_confidence(stage3)
    _apply_dual_rule(stage3)
    stages.append(stage3)

    # ── Stage 4: Fertilizer & Food Pressure ───────────────────────────────
    stage4 = CascadeStage(
        timeframe="Month 3-6",
        name="Fertilizer & Food Pressure",
        description="Urea/ammonia prices spike. Planting disrupted. Food inflation visible.",
        status="not_started",
        confidence=0.0,
        inputs_expected=8,  # zw, zc, dba, nitrogen_ppi, fert_equities, food_cpi, ppi, upstream
    )
    zw = proxy_data.get("ZW=F")
    zc = proxy_data.get("ZC=F")
    dba = proxy_data.get("DBA")
    zw_1m = _pct(zw)
    zc_1m = _pct(zc)
    dba_1m = _pct(dba)
    cpi_signal = _macro_signal(macro, "CPIAUCSL")
    ppi_signal = _macro_signal(macro, "PPIACO")
    food_at_home_signal = _macro_signal(macro, "CUSR0000SAF11")
    food_at_home_yoy = _macro_yoy(macro, "CUSR0000SAF11")
    nitrogen_ppi_signal = _macro_signal(macro, "WPU06790303")
    nitrogen_ppi_yoy = _macro_yoy(macro, "WPU06790303")

    # Grain futures
    if zw_1m is not None:
        stage4.inputs_received += 1
        if zw_1m > 10:
            stage4.stress_score += 0.25
            stage4.has_momentum = True
            stage4.evidence.append(f"Wheat +{zw_1m:.1f}% (1m)")
        elif zw_1m > 5:
            stage4.stress_score += 0.1
            stage4.evidence.append(f"Wheat +{zw_1m:.1f}% (1m)")
    if zc_1m is not None:
        stage4.inputs_received += 1
        if zc_1m > 10:
            stage4.stress_score += 0.15
            stage4.has_momentum = True
            stage4.evidence.append(f"Corn +{zc_1m:.1f}% (1m)")
    if dba_1m is not None:
        stage4.inputs_received += 1
        if dba_1m > 5:
            stage4.stress_score += 0.15
            stage4.has_momentum = True
            stage4.evidence.append(f"DBA (agriculture) +{dba_1m:.1f}% (1m)")

    # Nitrogen fertilizer PPI (new in v2)
    if nitrogen_ppi_signal is not None:
        stage4.inputs_received += 1
        if nitrogen_ppi_signal == "critical":
            stage4.stress_score += 0.35
            stage4.has_momentum = True
            stage4.evidence.append(
                f"Nitrogen fertilizer PPI: critical"
                + (f" ({nitrogen_ppi_yoy:+.1f}% YoY)" if nitrogen_ppi_yoy else "")
            )
        elif nitrogen_ppi_signal == "warning":
            stage4.stress_score += 0.25
            stage4.has_momentum = True
            stage4.evidence.append(
                f"Nitrogen fertilizer PPI: warning"
                + (f" ({nitrogen_ppi_yoy:+.1f}% YoY)" if nitrogen_ppi_yoy else "")
            )

    # Fertilizer equities composite (new in v2)
    fert_tickers = ["MOS", "NTR", "CF"]
    fert_down = 0
    fert_total = 0
    for t in fert_tickers:
        td = proxy_data.get(t)
        if td is not None:
            fert_total += 1
            p = _pct(td)
            if p is not None and p < -5:
                fert_down += 1
    if fert_total > 0:
        stage4.inputs_received += 1
        if fert_down >= 2:
            stage4.stress_score += 0.15
            stage4.has_momentum = True
            stage4.evidence.append(f"{fert_down}/{fert_total} fertilizer equities down >5% (1m)")

    # Food CPI
    if food_at_home_signal is not None:
        stage4.inputs_received += 1
        if food_at_home_signal == "critical":
            stage4.stress_score += 0.3
            stage4.has_momentum = True
            yoy_str = f" ({food_at_home_yoy:+.1f}% YoY)" if food_at_home_yoy else ""
            stage4.evidence.append(f"CPI Food at Home: critical{yoy_str} — grocery inflation accelerating")
        elif food_at_home_signal in ("warning", "bearish"):
            stage4.stress_score += 0.2
            yoy_str = f" ({food_at_home_yoy:+.1f}% YoY)" if food_at_home_yoy else ""
            stage4.evidence.append(f"CPI Food at Home: {food_at_home_signal}{yoy_str}")

    # Producer prices
    if ppi_signal is not None:
        stage4.inputs_received += 1
        if ppi_signal in ("warning", "critical"):
            stage4.stress_score += 0.15
            stage4.evidence.append(f"PPI: {ppi_signal} — producer input costs rising")

    # Upstream cascade
    if stage1.status == "active":
        stage4.inputs_received += 1
        stage4.stress_score += 0.1
        stage4.evidence.append("Oil shock active → fertilizer cost pressure")
    elif stage1.status == "projected":
        stage4.inputs_received += 1
        stage4.stress_score += 0.05

    _update_confidence(stage4)
    _apply_dual_rule(stage4)
    stages.append(stage4)

    # ── Stage 5: Pharmaceutical Delays ────────────────────────────────────
    stage5 = CascadeStage(
        timeframe="Month 4-8",
        name="Pharmaceutical Delays",
        description="India flags raw material shortages. Generic drug supply chains lengthen.",
        status="not_started",
        confidence=0.0,
        inputs_expected=5,  # upstream_count, indpro, inda, xph, fda_shortages
    )
    inda = proxy_data.get("INDA")
    inda_1m = _pct(inda)
    xph = proxy_data.get("XPH")
    xph_1m = _pct(xph)

    active_count_so_far = sum(1 for s in stages if s.status == "active")

    # Upstream cascade pressure
    stage5.inputs_received += 1
    if active_count_so_far >= 3:
        stage5.stress_score += 0.35
        stage5.has_momentum = True
        stage5.evidence.append(f"{active_count_so_far} upstream stages active — raw material pressure cascading")
    elif active_count_so_far >= 2:
        stage5.stress_score += 0.15
        stage5.evidence.append(f"{active_count_so_far} upstream stages active")

    if indpro_signal is not None:
        stage5.inputs_received += 1
        if indpro_signal in ("warning", "critical"):
            stage5.stress_score += 0.15
            stage5.evidence.append(f"Industrial production: {indpro_signal}")

    # India market proxy (new in v2)
    if inda_1m is not None:
        stage5.inputs_received += 1
        if inda_1m < -8:
            stage5.stress_score += 0.2
            stage5.has_momentum = True
            stage5.evidence.append(f"INDA (India ETF) {inda_1m:+.1f}% (1m) — Indian market stress")
        elif inda_1m < -4:
            stage5.stress_score += 0.1
            stage5.evidence.append(f"INDA {inda_1m:+.1f}% (1m)")

    # Pharma ETF (new in v2)
    if xph_1m is not None:
        stage5.inputs_received += 1
        if xph_1m < -8:
            stage5.stress_score += 0.2
            stage5.has_momentum = True
            stage5.evidence.append(f"XPH (pharma ETF) {xph_1m:+.1f}% (1m) — sector under pressure")
        elif xph_1m < -4:
            stage5.stress_score += 0.1
            stage5.evidence.append(f"XPH {xph_1m:+.1f}% (1m)")

    # openFDA shortages (new in v2)
    if fda_shortages is not None:
        stage5.inputs_received += 1
        if fda_shortages.new_last_30d > 50:
            stage5.stress_score += 0.3
            stage5.has_momentum = True
            stage5.evidence.append(
                f"openFDA: {fda_shortages.new_last_30d} new drug shortages (30d) — "
                f"{fda_shortages.total_active} total active"
            )
        elif fda_shortages.new_last_30d > 20:
            stage5.stress_score += 0.15
            stage5.evidence.append(
                f"openFDA: {fda_shortages.new_last_30d} new shortages (30d)"
            )

    _update_confidence(stage5)
    _apply_dual_rule(stage5)
    stages.append(stage5)

    # ── Stage 6: Industrial Slowdown ──────────────────────────────────────
    stage6 = CascadeStage(
        timeframe="Month 6-12",
        name="Industrial Slowdown",
        description="Petrochemical feedstock shortages. Manufacturing slows. Employment declines.",
        status="not_started",
        confidence=0.0,
        inputs_expected=7,  # upstream, indpro, tcu, manemp, dgorder, unrate, slx
    )
    tcu_val = _macro_val(macro, "TCU")
    tcu_signal = _macro_signal(macro, "TCU")
    manemp_signal = _macro_signal(macro, "MANEMP")
    manemp_change = _macro_change(macro, "MANEMP")
    dgorder_signal_6 = _macro_signal(macro, "DGORDER")
    unrate_signal = _macro_signal(macro, "UNRATE")
    slx = proxy_data.get("SLX")
    slx_1m = _pct(slx)

    active_count_all = sum(1 for s in stages if s.status == "active")

    # Upstream cascade
    stage6.inputs_received += 1
    if active_count_all >= 4:
        stage6.stress_score += 0.3
        stage6.has_momentum = True
        stage6.evidence.append(f"{active_count_all} upstream stages active — broad cascade underway")
    elif active_count_all >= 3:
        stage6.stress_score += 0.2
        stage6.evidence.append(f"{active_count_all} upstream stages active")

    # Manufacturing health composite (replaces PMI — see plan)
    if indpro_signal is not None:
        stage6.inputs_received += 1
        if indpro_signal == "critical":
            stage6.stress_score += 0.3
            stage6.has_momentum = True
            stage6.evidence.append("Industrial production contracting sharply")
        elif indpro_signal == "warning":
            stage6.stress_score += 0.15
            stage6.has_momentum = True
            stage6.evidence.append("Industrial production declining")

    if tcu_signal is not None:
        stage6.inputs_received += 1
        if tcu_signal in ("critical", "warning"):
            stage6.stress_score += 0.2
            stage6.has_momentum = True
            stage6.evidence.append(
                f"Capacity utilization: {tcu_val:.1f}% ({tcu_signal})"
                if tcu_val else f"Capacity utilization: {tcu_signal}"
            )

    if manemp_signal is not None:
        stage6.inputs_received += 1
        if manemp_signal in ("critical", "warning"):
            stage6.stress_score += 0.2
            stage6.has_momentum = True
            stage6.evidence.append(
                f"Manufacturing employment: {manemp_signal}"
                + (f" ({manemp_change:+.0f}K)" if manemp_change else "")
            )

    if dgorder_signal_6 is not None:
        stage6.inputs_received += 1
        if dgorder_signal_6 in ("warning", "critical"):
            stage6.stress_score += 0.15
            stage6.has_momentum = True
            stage6.evidence.append(f"Durable goods orders: {dgorder_signal_6}")

    if unrate_signal is not None:
        stage6.inputs_received += 1
        if unrate_signal in ("warning", "critical"):
            stage6.stress_score += 0.15
            stage6.evidence.append(f"Unemployment: {unrate_signal}")

    # Steel/materials proxy (new in v2)
    if slx_1m is not None:
        stage6.inputs_received += 1
        if slx_1m < -8:
            stage6.stress_score += 0.15
            stage6.has_momentum = True
            stage6.evidence.append(f"SLX (steel ETF) {slx_1m:+.1f}% (1m) — materials sector weakening")
        elif slx_1m < -4:
            stage6.stress_score += 0.1
            stage6.evidence.append(f"SLX {slx_1m:+.1f}% (1m)")

    _update_confidence(stage6)
    _apply_dual_rule(stage6)
    stages.append(stage6)

    # ── Convergence amplifier ─────────────────────────────────────────────
    moderate_stages = [s for s in stages if s.stress_score >= CONVERGENCE_MIN_SCORE]
    if len(moderate_stages) >= CONVERGENCE_THRESHOLD:
        for s in moderate_stages:
            s.stress_score = min(s.stress_score + CONVERGENCE_BOOST, 1.5)
            s.evidence.append(
                f"Convergence amplifier: {len(moderate_stages)} stages at moderate+ stress"
            )
        # Re-apply dual-rule after convergence boost
        for s in stages:
            _apply_dual_rule(s)

    # ── Timeline anchoring ────────────────────────────────────────────────
    crisis_start = _resolve_crisis_start(hormuz, config)

    # Fallback: use historical peak date if any stage is active
    if crisis_start is None and any(s.status == "active" for s in stages):
        try:
            from ..personal.historical import _2026_PEAK_DATE
            crisis_start = _2026_PEAK_DATE
        except ImportError:
            pass

    if crisis_start is not None:
        _compute_stage_dates(crisis_start, stages)
    else:
        for i, s in enumerate(stages):
            if i < len(STAGE_RELATIVE_LABELS):
                s.timeframe = STAGE_RELATIVE_LABELS[i]

    # Annotate with historical first-activation dates from JSONL
    transitions = _load_stage_transitions()
    for s in stages:
        first = transitions.get(s.name)
        if first is not None:
            s.first_activated_date = first

    return stages


def persist_cascade_snapshot(stages: list[CascadeStage]) -> Path | None:
    """Append today's cascade evaluation to the history JSONL file."""
    try:
        HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "date": date.today().isoformat(),
            "timestamp": datetime.utcnow().isoformat(),
            "stages": [asdict(s) for s in stages],
        }
        with open(HISTORY_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")
        return HISTORY_PATH
    except Exception:
        return None
