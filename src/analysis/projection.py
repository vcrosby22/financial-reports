"""Forward-looking risk projection and bottom-estimate model.

Combines risk score trajectory, macro net-stress ratio, and supply chain
cascade momentum into a directional forecast:
  WORSENING / STRESSED, HOLDING / STABLE / EASING.

Direction is determined purely by trajectory (are things getting worse?)
rather than absolute stress level (how bad are things now?).  High absolute
risk scores shift the label to STRESSED, HOLDING when trajectory is flat,
but do NOT inflate the direction score itself.

Confidence blends data completeness (50%) with signal-family convergence
(50%) — three families agreeing on direction gives higher confidence than
mixed signals.

Also provides a bottom-estimate model that uses historical analog crashes
(weighted by crisis factor overlap) to project where the current decline
might end: optimistic, base, and pessimistic scenarios.

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
        return {
            "worsening": "WORSENING",
            "easing": "EASING",
            "stressed_holding": "STRESSED, HOLDING",
            "stable": "STABLE",
        }.get(self.direction, self.direction.upper())

    @property
    def color_var(self) -> str:
        return {
            "worsening": "var(--red)",
            "stressed_holding": "var(--orange, var(--yellow))",
            "stable": "var(--yellow)",
            "easing": "var(--green)",
        }.get(self.direction, "var(--text-dim)")


def _macro_net_stress(macro: MacroSnapshot) -> tuple[float, int, dict[str, int]]:
    """Compute a normalized net stress ratio from all macro indicators.

    Returns (ratio, indicator_count, signal_counts).
    Ratio range: roughly -1.0 (all bullish) to +2.0 (all critical).
    """
    weights = {"critical": 2, "warning": 1, "bearish": 0.5, "neutral": 0, "bullish": -1}
    total_weight = 0.0
    counts: dict[str, int] = {"critical": 0, "warning": 0, "bearish": 0, "neutral": 0, "bullish": 0}
    n = 0
    for ind in macro.indicators:
        sig = getattr(ind, "signal", "neutral") or "neutral"
        counts[sig] = counts.get(sig, 0) + 1
        total_weight += weights.get(sig, 0)
        n += 1
    ratio = total_weight / n if n > 0 else 0.0
    return ratio, n, counts


def compute_projection(
    risk_trend: RiskTrend | None,
    macro: MacroSnapshot | None,
    cascade_active_count: int = 0,
) -> RiskProjection:
    """Compute a directional risk projection from available signals.

    Returns a RiskProjection with direction, confidence, and supporting factors.
    Confidence blends data completeness (do we have enough data?) with signal
    convergence (do the signal families agree on direction?).
    """
    score = 0.0  # positive = worsening, negative = improving
    factors: list[str] = []
    data_points = 0
    current_uncapped = risk_trend.current_uncapped if risk_trend else 0

    # Per-family directional votes for convergence scoring.
    # +1 = worsening, -1 = easing, 0 = neutral.
    family_votes: list[int] = []

    # --- Risk score trajectory (highest weight when data exists) ---
    trajectory_sub = 0.0
    if risk_trend and risk_trend.has_any:
        if risk_trend.delta_1d is not None:
            data_points += 1
            if risk_trend.delta_1d > 3:
                trajectory_sub += 1.5
                factors.append(f"Risk score up {risk_trend.delta_1d:+d} in 24h")
            elif risk_trend.delta_1d < -3:
                trajectory_sub -= 1.5
                factors.append(f"Risk score down {risk_trend.delta_1d:+d} in 24h")

        if risk_trend.delta_1w is not None:
            data_points += 1
            if risk_trend.delta_1w > 5:
                trajectory_sub += 2.0
                factors.append(f"Risk score up {risk_trend.delta_1w:+d} over 1 week")
            elif risk_trend.delta_1w > 0:
                trajectory_sub += 0.5
                factors.append(f"Risk score trending up over 1 week ({risk_trend.delta_1w:+d})")
            elif risk_trend.delta_1w < -5:
                trajectory_sub -= 2.0
                factors.append(f"Risk score down {risk_trend.delta_1w:+d} over 1 week")
            elif risk_trend.delta_1w < 0:
                trajectory_sub -= 0.5
                factors.append(f"Risk score trending down over 1 week ({risk_trend.delta_1w:+d})")

        if risk_trend.delta_1m is not None:
            data_points += 1
            if risk_trend.delta_1m > 10:
                trajectory_sub += 2.5
                factors.append(f"Risk score up {risk_trend.delta_1m:+d} over 1 month — sustained deterioration")
            elif risk_trend.delta_1m < -10:
                trajectory_sub -= 2.5
                factors.append(f"Risk score down {risk_trend.delta_1m:+d} over 1 month — sustained improvement")

    score += trajectory_sub
    if trajectory_sub > 0.5:
        family_votes.append(1)
    elif trajectory_sub < -0.5:
        family_votes.append(-1)
    elif risk_trend and risk_trend.has_any:
        family_votes.append(0)

    # --- Macro indicators: net stress ratio (includes bearish signals) ---
    macro_sub = 0.0
    if macro and macro.indicators:
        ratio, n_ind, counts = _macro_net_stress(macro)
        data_points += n_ind

        if ratio > 0.4:
            macro_sub = min(ratio / 0.4, 1.0) * 2.5
        elif ratio > 0:
            macro_sub = (ratio / 0.4) * 1.0
        elif ratio < -0.3:
            macro_sub = max(ratio / 0.3, -1.0) * 1.5
        elif ratio < 0:
            macro_sub = (ratio / 0.3) * 0.5

        stress_parts = []
        for level in ("critical", "warning", "bearish"):
            if counts.get(level, 0):
                stress_parts.append(f"{counts[level]} {level}")
        if counts.get("bullish", 0):
            stress_parts.append(f"{counts['bullish']} bullish")

        summary = ", ".join(stress_parts) if stress_parts else "all neutral"
        factors.append(f"Macro net stress {ratio:+.2f} — {summary} across {n_ind} indicators")

    score += macro_sub
    if macro_sub > 0.3:
        family_votes.append(1)
    elif macro_sub < -0.3:
        family_votes.append(-1)
    elif macro and macro.indicators:
        family_votes.append(0)

    # --- Supply chain cascade momentum (6-stage model, symmetric) ---
    cascade_sub = 0.0
    data_points += 1
    if cascade_active_count >= 3:
        cascade_sub = 2.0
        factors.append(f"Broad supply chain cascade: {cascade_active_count}/6 stages active")
    elif cascade_active_count >= 2:
        cascade_sub = 1.0
        factors.append(f"Supply chain cascade building: {cascade_active_count}/6 stages active")
    elif cascade_active_count == 1:
        cascade_sub = 0.3
        factors.append("1 supply chain cascade stage active")
    else:
        cascade_sub = -0.3
        factors.append("No active supply chain cascade stages")

    score += cascade_sub
    if cascade_sub > 0.1:
        family_votes.append(1)
    elif cascade_sub < -0.1:
        family_votes.append(-1)
    else:
        family_votes.append(0)

    # --- Direction: absolute level informs label, NOT direction score ---
    if score > 2.5:
        direction = "worsening"
    elif score < -2.5:
        direction = "easing"
    elif current_uncapped >= 150:
        direction = "stressed_holding"
    else:
        direction = "stable"

    # --- Confidence: data completeness (50%) + signal convergence (50%) ---
    completeness = min(data_points / 8, 1.0)

    non_neutral = [v for v in family_votes if v != 0]
    if len(non_neutral) >= 2:
        majority = 1 if sum(non_neutral) > 0 else (-1 if sum(non_neutral) < 0 else 0)
        if majority != 0:
            convergence = sum(1 for v in non_neutral if v == majority) / len(non_neutral)
        else:
            convergence = 0.0
    elif len(non_neutral) == 1:
        convergence = 0.5
    else:
        convergence = 0.0

    confidence = (completeness * 0.5) + (convergence * 0.5)

    if data_points == 0:
        factors.append("Limited historical data — projection confidence will improve over time")

    if not factors:
        factors.append("Insufficient signals for directional call — monitoring")

    return RiskProjection(direction=direction, confidence=confidence, factors=factors)


@dataclass
class BottomEstimate:
    """Projected bottom range based on historical analog crashes."""
    peak_level: float
    current_level: float
    current_decline_pct: float
    optimistic_decline: float   # shallowest analog decline (%)
    base_decline: float         # factor-weighted average decline (%)
    pessimistic_decline: float  # deepest analog decline (%)
    optimistic_level: float     # S&P 500 level at optimistic bottom
    base_level: float
    pessimistic_level: float
    optimistic_days: int        # estimated days to bottom
    base_days: int
    pessimistic_days: int
    analogs_used: list[str] = field(default_factory=list)
    confidence: float = 0.0


def compute_bottom_estimate(
    sp500_price: float | None,
    similar_crashes: list,
    current_factors: set[str],
    peak: float = 6900.0,
) -> BottomEstimate | None:
    """Project where the bottom might be using factor-weighted historical analogs.

    Uses the top historical matches (by factor overlap) to produce three
    scenarios. Each analog's decline is weighted by its factor overlap count
    with the current crisis.
    """
    if not similar_crashes or not current_factors:
        return None

    price = sp500_price if sp500_price else peak
    current_decline = ((price - peak) / peak) * 100

    analogs = similar_crashes[:5]

    weighted_decline = 0.0
    weighted_days = 0.0
    total_weight = 0.0
    declines = []
    days_list = []
    names = []

    for crash in analogs:
        overlap = len(crash.crisis_factors & current_factors)
        if overlap == 0:
            continue
        weight = overlap ** 2
        weighted_decline += crash.decline_pct * weight
        weighted_days += crash.days_to_bottom * weight
        total_weight += weight
        declines.append(crash.decline_pct)
        days_list.append(crash.days_to_bottom)
        names.append(crash.name)

    if total_weight == 0 or not declines:
        return None

    base_decline = weighted_decline / total_weight
    base_days = int(weighted_days / total_weight)
    optimistic_decline = max(declines)  # least negative = shallowest
    pessimistic_decline = min(declines)  # most negative = deepest
    optimistic_days = min(days_list)
    pessimistic_days = max(days_list)

    return BottomEstimate(
        peak_level=peak,
        current_level=price,
        current_decline_pct=round(current_decline, 1),
        optimistic_decline=round(optimistic_decline, 1),
        base_decline=round(base_decline, 1),
        pessimistic_decline=round(pessimistic_decline, 1),
        optimistic_level=round(peak * (1 + optimistic_decline / 100)),
        base_level=round(peak * (1 + base_decline / 100)),
        pessimistic_level=round(peak * (1 + pessimistic_decline / 100)),
        optimistic_days=optimistic_days,
        base_days=base_days,
        pessimistic_days=pessimistic_days,
        analogs_used=names,
        confidence=min(0.2 * len(names), 0.8),
    )
