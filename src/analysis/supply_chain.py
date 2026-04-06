"""Dynamic supply chain cascade engine.

Evaluates each cascade stage against live market data (yfinance proxies)
and FRED macro indicators. Replaces the hardcoded supply chain status
in the report with data-driven activation detection.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path

from ..data.macro import MacroSnapshot


HISTORY_PATH = Path(__file__).parent.parent.parent / "data" / "supply_chain_history.jsonl"


@dataclass
class CascadeStage:
    timeframe: str
    name: str
    description: str
    status: str  # "active", "projected", "not_started"
    confidence: float  # 0.0 – 1.0
    evidence: list[str] = field(default_factory=list)


def _pct(data: dict | None, key: str = "change_pct_1m") -> float | None:
    """Safely extract a percentage change from a ticker dict."""
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
    """Get the latest value for a FRED series from the macro snapshot."""
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
    """Get the year-over-year change for a FRED series."""
    if macro is None:
        return None
    for ind in macro.indicators:
        if ind.series_id == series_id:
            return getattr(ind, "yoy_change", None)
    return None


def evaluate_cascade(
    proxy_data: dict[str, dict],
    macro: MacroSnapshot | None,
    commodities: list[dict] | None = None,
) -> list[CascadeStage]:
    """Evaluate all cascade stages and return live status.

    Args:
        proxy_data: dict mapping ticker (e.g. "NG=F") to its fetch_ticker_data result.
        macro: current MacroSnapshot with FRED indicators.
        commodities: list of commodity dicts from the main report pipeline.
    """
    commodity_map: dict[str, dict] = {}
    if commodities:
        for c in commodities:
            commodity_map[c.get("ticker", "")] = c

    stages: list[CascadeStage] = []

    # --- Stage 1: Oil Price Shock (Week 1-2) ---
    stage1 = CascadeStage(
        timeframe="Week 1-2",
        name="Oil Price Shock",
        description="Brent crude spikes, WTI follows. Gasoline prices at pump rise within days.",
        status="not_started",
        confidence=0.0,
    )
    brent_fred_signal = _macro_signal(macro, "DCOILBRENTEU")
    brent_price = _macro_val(macro, "DCOILBRENTEU")
    bz = commodity_map.get("BZ=F")
    cl = commodity_map.get("CL=F")
    bz_1m = _pct(bz)
    cl_1m = _pct(cl)

    oil_evidence: list[str] = []
    oil_score = 0.0
    if brent_price and brent_price >= 85:
        oil_score += 0.4
        oil_evidence.append(f"Brent at ${brent_price:.0f}")
    if brent_fred_signal in ("warning", "critical"):
        oil_score += 0.3
        oil_evidence.append(f"Brent FRED signal: {brent_fred_signal}")
    if bz_1m is not None and bz_1m > 10:
        oil_score += 0.3
        oil_evidence.append(f"Brent futures +{bz_1m:.1f}% (1m)")
    if cl_1m is not None and cl_1m > 10:
        oil_score += 0.2
        oil_evidence.append(f"WTI futures +{cl_1m:.1f}% (1m)")

    stage1.confidence = min(oil_score, 1.0)
    stage1.evidence = oil_evidence
    stage1.status = "active" if oil_score >= 0.5 else ("projected" if oil_score >= 0.2 else "not_started")
    stages.append(stage1)

    # --- Stage 2: Energy Cost Cascade (Month 1-2) ---
    stage2 = CascadeStage(
        timeframe="Month 1-2",
        name="Energy Cost Cascade",
        description="Natural gas prices spike (LNG rerouting). Electricity costs rise. Industrial production slows.",
        status="not_started",
        confidence=0.0,
    )
    ng = proxy_data.get("NG=F")
    ng_1m = _pct(ng)
    gas_price = _macro_val(macro, "GASREGW")
    gas_signal = _macro_signal(macro, "GASREGW")
    indpro_signal = _macro_signal(macro, "INDPRO")

    energy_evidence: list[str] = []
    energy_score = 0.0
    if ng_1m is not None and ng_1m > 15:
        energy_score += 0.4
        energy_evidence.append(f"Nat gas +{ng_1m:.1f}% (1m)")
    elif ng_1m is not None and ng_1m > 5:
        energy_score += 0.2
        energy_evidence.append(f"Nat gas +{ng_1m:.1f}% (1m)")
    if gas_signal in ("warning", "critical"):
        energy_score += 0.3
        energy_evidence.append(f"Gasoline: ${gas_price:.2f}/gal ({gas_signal})")
    if indpro_signal in ("warning", "critical"):
        energy_score += 0.3
        energy_evidence.append(f"Industrial production: {indpro_signal}")

    stage2.confidence = min(energy_score, 1.0)
    stage2.evidence = energy_evidence
    stage2.status = "active" if energy_score >= 0.5 else ("projected" if energy_score >= 0.2 else "not_started")
    stages.append(stage2)

    # --- Stage 3: Semiconductor Squeeze (Month 2-4) ---
    stage3 = CascadeStage(
        timeframe="Month 2-4",
        name="Helium & Semiconductor Squeeze",
        description="Semiconductor fabs reduce output. Lead times extend. Helium spot prices surge.",
        status="not_started",
        confidence=0.0,
    )
    soxx = proxy_data.get("SOXX")
    soxx_1m = _pct(soxx)
    hg = proxy_data.get("HG=F")
    hg_1m = _pct(hg)

    semi_evidence: list[str] = []
    semi_score = 0.0
    if soxx_1m is not None and soxx_1m < -10:
        semi_score += 0.5
        semi_evidence.append(f"SOXX {soxx_1m:+.1f}% (1m) — chip sector under pressure")
    elif soxx_1m is not None and soxx_1m < -5:
        semi_score += 0.25
        semi_evidence.append(f"SOXX {soxx_1m:+.1f}% (1m)")
    if hg_1m is not None and abs(hg_1m) > 8:
        semi_score += 0.2
        semi_evidence.append(f"Copper {hg_1m:+.1f}% (1m) — industrial demand signal")
    if stage2.status == "active":
        semi_score += 0.2
        semi_evidence.append("Energy cascade active → downstream pressure building")

    stage3.confidence = min(semi_score, 1.0)
    stage3.evidence = semi_evidence
    stage3.status = "active" if semi_score >= 0.5 else ("projected" if semi_score >= 0.2 else "not_started")
    stages.append(stage3)

    # --- Stage 4: Fertilizer & Food Pressure (Month 3-6) ---
    stage4 = CascadeStage(
        timeframe="Month 3-6",
        name="Fertilizer & Food Pressure",
        description="Urea/ammonia prices spike. Planting disrupted. Food inflation visible.",
        status="not_started",
        confidence=0.0,
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

    food_evidence: list[str] = []
    food_score = 0.0
    if zw_1m is not None and zw_1m > 10:
        food_score += 0.3
        food_evidence.append(f"Wheat +{zw_1m:.1f}% (1m)")
    if zc_1m is not None and zc_1m > 10:
        food_score += 0.2
        food_evidence.append(f"Corn +{zc_1m:.1f}% (1m)")
    if dba_1m is not None and dba_1m > 5:
        food_score += 0.2
        food_evidence.append(f"DBA (agriculture) +{dba_1m:.1f}% (1m)")
    if food_at_home_signal == "critical":
        food_score += 0.35
        food_evidence.append(f"CPI Food at Home: critical (YoY {food_at_home_yoy:+.1f}%) — grocery inflation accelerating" if food_at_home_yoy else "CPI Food at Home: critical — grocery inflation accelerating")
    elif food_at_home_signal in ("warning", "bearish"):
        food_score += 0.25
        food_evidence.append(f"CPI Food at Home: {food_at_home_signal} (YoY {food_at_home_yoy:+.1f}%) — grocery prices rising above target" if food_at_home_yoy else f"CPI Food at Home: {food_at_home_signal} — grocery prices rising above target")
    if ppi_signal in ("warning", "critical"):
        food_score += 0.2
        food_evidence.append(f"PPI: {ppi_signal} — producer input costs rising")
    if cpi_signal in ("warning", "critical"):
        food_score += 0.15
        food_evidence.append(f"CPI: {cpi_signal} — consumer prices elevated")
    if stage1.status == "active":
        food_score += 0.1
        food_evidence.append("Oil shock active → fertilizer cost pressure")

    stage4.confidence = min(food_score, 1.0)
    stage4.evidence = food_evidence
    stage4.status = "active" if food_score >= 0.5 else ("projected" if food_score >= 0.2 else "not_started")
    stages.append(stage4)

    # --- Stage 5: Pharmaceutical Delays (Month 4-8) ---
    stage5 = CascadeStage(
        timeframe="Month 4-8",
        name="Pharmaceutical Delays",
        description="India flags raw material shortages. Generic drug supply chains lengthen.",
        status="not_started",
        confidence=0.0,
    )
    pharma_evidence: list[str] = []
    pharma_score = 0.0
    active_count = sum(1 for s in stages if s.status == "active")
    if active_count >= 3:
        pharma_score += 0.4
        pharma_evidence.append(f"{active_count} upstream stages active — raw material pressure cascading")
    elif active_count >= 2:
        pharma_score += 0.2
        pharma_evidence.append(f"{active_count} upstream stages active")
    if indpro_signal in ("warning", "critical"):
        pharma_score += 0.2
        pharma_evidence.append(f"Industrial production: {indpro_signal}")

    stage5.confidence = min(pharma_score, 1.0)
    stage5.evidence = pharma_evidence
    stage5.status = "active" if pharma_score >= 0.5 else ("projected" if pharma_score >= 0.2 else "not_started")
    stages.append(stage5)

    # --- Stage 6: Industrial Slowdown (Month 6-12) ---
    stage6 = CascadeStage(
        timeframe="Month 6-12",
        name="Industrial Slowdown",
        description="Petrochemical feedstock shortages. Plastics/packaging costs rise. Manufacturing slows.",
        status="not_started",
        confidence=0.0,
    )
    industrial_evidence: list[str] = []
    industrial_score = 0.0
    if active_count >= 4:
        industrial_score += 0.3
        industrial_evidence.append(f"{active_count} upstream stages active — broad cascade underway")
    if indpro_signal == "critical":
        industrial_score += 0.4
        industrial_evidence.append("Industrial production contracting sharply")
    elif indpro_signal == "warning":
        industrial_score += 0.2
        industrial_evidence.append("Industrial production declining")
    unrate_signal = _macro_signal(macro, "UNRATE")
    if unrate_signal in ("warning", "critical"):
        industrial_score += 0.2
        industrial_evidence.append(f"Unemployment: {unrate_signal}")

    stage6.confidence = min(industrial_score, 1.0)
    stage6.evidence = industrial_evidence
    stage6.status = "active" if industrial_score >= 0.5 else ("projected" if industrial_score >= 0.2 else "not_started")
    stages.append(stage6)

    # --- Stage 7 & 8: Longer-term (always projected unless later stages activate) ---
    stages.append(CascadeStage(
        timeframe="Year 1-3",
        name="Infrastructure Rebuild",
        description="Even after ceasefire, Gulf infrastructure requires years to rebuild. LNG/helium supply constrained.",
        status="projected",
        confidence=0.15,
        evidence=["Long-term structural — always projected until resolution confirmed"],
    ))
    stages.append(CascadeStage(
        timeframe="Year 3-5+",
        name="New Supply Equilibrium",
        description="Alternative supply chains mature. New plants reach capacity. Markets find new equilibrium.",
        status="projected",
        confidence=0.1,
        evidence=["Terminal stage — projected until upstream stages resolve"],
    ))

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
