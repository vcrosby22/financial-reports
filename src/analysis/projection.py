"""Forward-looking risk projection.

Combines risk score trajectory, leading macro indicators, and supply chain
momentum into a directional forecast: WORSENING / STABLE / IMPROVING.

This module is designed to become more valuable as historical data
(risk_score_history.jsonl, supply_chain_history.jsonl) accumulates.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..data.macro import MacroSnapshot
from ..data.risk_score_log import RiskTrend


@dataclass
class RiskProjection:
    direction: str  # "worsening", "stable", "improving"
    confidence: float  # 0.0 – 1.0
    factors: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        return self.direction.upper()

    @property
    def color_var(self) -> str:
        return {
            "worsening": "var(--red)",
            "stable": "var(--yellow)",
            "improving": "var(--green)",
        }.get(self.direction, "var(--text-dim)")


def compute_projection(
    risk_trend: RiskTrend | None,
    macro: MacroSnapshot | None,
    cascade_active_count: int = 0,
) -> RiskProjection:
    """Compute a directional risk projection from available signals.

    Returns a RiskProjection with direction, confidence, and supporting factors.
    Confidence reflects data completeness — more historical data = higher confidence.
    """
    score = 0.0  # positive = worsening, negative = improving
    factors: list[str] = []
    data_points = 0

    # --- Risk score trajectory (highest weight when data exists) ---
    if risk_trend and risk_trend.has_any:
        if risk_trend.delta_1d is not None:
            data_points += 1
            if risk_trend.delta_1d > 3:
                score += 1.5
                factors.append(f"Risk score up {risk_trend.delta_1d:+d} in 24h")
            elif risk_trend.delta_1d < -3:
                score -= 1.5
                factors.append(f"Risk score down {risk_trend.delta_1d:+d} in 24h")

        if risk_trend.delta_1w is not None:
            data_points += 1
            if risk_trend.delta_1w > 5:
                score += 2.0
                factors.append(f"Risk score up {risk_trend.delta_1w:+d} over 1 week")
            elif risk_trend.delta_1w > 0:
                score += 0.5
                factors.append(f"Risk score trending up over 1 week ({risk_trend.delta_1w:+d})")
            elif risk_trend.delta_1w < -5:
                score -= 2.0
                factors.append(f"Risk score down {risk_trend.delta_1w:+d} over 1 week")
            elif risk_trend.delta_1w < 0:
                score -= 0.5
                factors.append(f"Risk score trending down over 1 week ({risk_trend.delta_1w:+d})")

        if risk_trend.delta_1m is not None:
            data_points += 1
            if risk_trend.delta_1m > 10:
                score += 2.5
                factors.append(f"Risk score up {risk_trend.delta_1m:+d} over 1 month — sustained deterioration")
            elif risk_trend.delta_1m < -10:
                score -= 2.5
                factors.append(f"Risk score down {risk_trend.delta_1m:+d} over 1 month — sustained improvement")

    # --- Leading macro indicators ---
    if macro:
        leading_series = ["T10Y2Y", "T10Y3M", "ICSA", "UMCSENT", "UNRATE"]
        warning_count = 0
        improving_count = 0
        for ind in macro.indicators:
            if ind.series_id in leading_series:
                data_points += 1
                if ind.signal in ("critical", "warning"):
                    warning_count += 1
                elif ind.signal == "bullish":
                    improving_count += 1

        if warning_count >= 3:
            score += 2.0
            factors.append(f"{warning_count} leading indicators at warning/critical")
        elif warning_count >= 2:
            score += 1.0
            factors.append(f"{warning_count} leading indicators elevated")
        if improving_count >= 2:
            score -= 1.0
            factors.append(f"{improving_count} leading indicators improving")

        inflation_series = ["CPIAUCSL", "PPIACO", "GASREGW"]
        inf_warnings = 0
        for ind in macro.indicators:
            if ind.series_id in inflation_series and ind.signal in ("warning", "critical"):
                inf_warnings += 1
        if inf_warnings >= 2:
            score += 1.5
            factors.append(f"Inflation pressure: {inf_warnings} price indicators elevated")
            data_points += 1

    # --- Supply chain cascade momentum ---
    if cascade_active_count > 0:
        data_points += 1
        if cascade_active_count >= 4:
            score += 2.0
            factors.append(f"Broad supply chain cascade: {cascade_active_count} stages active")
        elif cascade_active_count >= 2:
            score += 1.0
            factors.append(f"Supply chain cascade building: {cascade_active_count} stages active")
        elif cascade_active_count == 1:
            score += 0.3
            factors.append("1 supply chain cascade stage active")

    # --- Determine direction ---
    if score > 1.5:
        direction = "worsening"
    elif score < -1.5:
        direction = "improving"
    else:
        direction = "stable"

    # --- Confidence based on data completeness ---
    if data_points >= 6:
        confidence = 0.75
    elif data_points >= 3:
        confidence = 0.5
    elif data_points >= 1:
        confidence = 0.3
    else:
        confidence = 0.1
        factors.append("Limited historical data — projection confidence will improve over time")

    if not factors:
        factors.append("Insufficient signals for directional call — monitoring")

    return RiskProjection(direction=direction, confidence=confidence, factors=factors)
