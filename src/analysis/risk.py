"""Risk scoring engine — monitors market health and flags collapse indicators.

Implements rule-based checks against known risk thresholds,
independent of the AI analysis layer. These are the "hard guardrails"
that trigger alerts even without Claude.

Layers:
  1. Technical signals (VIX, death crosses, RSI, breadth)
  2. Macroeconomic signals (yield curve, credit spreads, unemployment)
  3. Fundamental signals (earnings revisions, insider activity, debt)
  4. Confidence scoring (how complete is our data?)
"""

import math
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class RiskSignal:
    name: str
    severity: str  # info, warning, critical
    category: str  # technical, volatility, macro, fundamental
    message: str
    ticker: str = ""
    value: float = 0.0
    signal_type: str = "lagging"  # lagging or leading


@dataclass
class ScoreContribution:
    """One signal's contribution to the additive risk score (before cap)."""

    name: str
    severity: str
    category: str
    signal_type: str
    ticker: str
    points: int


@dataclass
class MarketHealthReport:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    overall_risk: str = "low"  # low, moderate, elevated, high, critical
    signals: list[RiskSignal] = field(default_factory=list)
    score: int = 0  # 0-100, higher = more risk (capped)
    score_uncapped: int = 0  # raw sum before min(..., 100)
    score_contributions: list[ScoreContribution] = field(default_factory=list)
    confidence: str = "low"  # low, medium, high — data completeness / coverage, not P(risk label)
    data_sources_present: list[str] = field(default_factory=list)
    data_sources_missing: list[str] = field(default_factory=list)

    @property
    def critical_count(self) -> int:
        return sum(1 for s in self.signals if s.severity == "critical")

    @property
    def warning_count(self) -> int:
        return sum(1 for s in self.signals if s.severity == "warning")

    @property
    def leading_signal_count(self) -> int:
        return sum(1 for s in self.signals if s.signal_type == "leading")


_CRISIS_EXPLANATION = (
    "Multiple severe risk signals are firing. The guidance is to hold cash and avoid "
    "opening new positions entirely. There is no stop-loss to set because the recommendation "
    "is to not buy right now. Focus on preserving what you have."
)

DISPLAY_LABELS: dict[str, str] = {
    "low": "LOW",
    "moderate": "MODERATE",
    "elevated": "ELEVATED",
    "high": "HIGH",
    "acute_stress": "ACUTE STRESS",
    "compounding_stress": "COMPOUNDING STRESS",
    "severe_stress": "SEVERE STRESS",
    "heavy_stress": "HEAVY STRESS",
}

LEGACY_SLUG_MAP: dict[str, str] = {
    "critical": "acute_stress",
    "severe": "compounding_stress",
    "extreme": "severe_stress",
    "catastrophic": "heavy_stress",
}


def display_label(slug: str) -> str:
    """Map an internal slug (current or legacy) to the reader-facing label."""
    canonical = LEGACY_SLUG_MAP.get(slug, slug)
    return DISPLAY_LABELS.get(canonical, canonical.upper())


def direction_word(delta_1d: int | None) -> str | None:
    """Map a 1-day score delta to a direction subbucket label."""
    if delta_1d is None:
        return None
    if delta_1d > 30:
        return "Increasing Rapidly"
    if delta_1d >= 4:
        return "Increasing Incrementally"
    if delta_1d >= -3:
        return "Holding"
    if delta_1d >= -30:
        return "Easing Incrementally"
    return "Easing Rapidly"


POSITION_SIZING = {
    "low": {
        "max_position": "3–5%",
        "stop_loss": "10–15% below entry",
        "explanation": (
            "Market conditions are calm. You can invest normally — put up to 3–5% of your "
            "total portfolio into any single new position. Set a stop-loss 10–15% below your "
            "buy price so you automatically sell if it drops that far."
        ),
    },
    "moderate": {
        "max_position": "1–3%",
        "stop_loss": "8–10% below entry",
        "explanation": (
            "Some risk signals are present. Keep new positions smaller — 1–3% of your portfolio each. "
            "Set a tighter stop-loss (8–10% below entry) to limit downside if conditions worsen."
        ),
    },
    "elevated": {
        "max_position": "1–2%",
        "stop_loss": "5–8% below entry",
        "explanation": (
            "The market is showing strain. Only open small positions (1–2% of portfolio) and set "
            "tight stop-losses (5–8% below entry). Favor quality holdings over speculative ones."
        ),
    },
    "high": {
        "max_position": "0.5–1%",
        "stop_loss": "5% below entry",
        "explanation": (
            "Significant risk across multiple categories. Keep any new positions very small "
            "(0.5–1% of portfolio) with a tight 5% stop-loss. Consider whether new positions "
            "are necessary at all right now."
        ),
    },
    "acute_stress": {"max_position": "0%", "stop_loss": "N/A", "explanation": _CRISIS_EXPLANATION},
    "compounding_stress": {"max_position": "0%", "stop_loss": "N/A", "explanation": _CRISIS_EXPLANATION + " Multiple compounding crises detected."},
    "severe_stress": {"max_position": "0%", "stop_loss": "N/A", "explanation": _CRISIS_EXPLANATION + " Broad systemic failure signals present."},
    "heavy_stress": {"max_position": "0%", "stop_loss": "N/A", "explanation": _CRISIS_EXPLANATION + " Unprecedented convergence of risk signals."},
}

POSITION_SIZING_BY_SCORE = [
    (3, "3-5% of portfolio", "10-15% below entry"),
    (6, "1-3% of portfolio", "8-10% below entry"),
    (10, "0.5-1% of portfolio", "5-8% below entry"),
]

SHORT_POSITION_SIZING = "1% max of portfolio with defined buy-stop"


def assess_market_health(
    market_data: dict,
    thresholds: dict,
    macro_data=None,
    fundamentals_data: dict | None = None,
) -> MarketHealthReport:
    """Run all risk checks against current market data.

    Args:
        market_data: Price/technical data from all asset classes
        thresholds: Alert thresholds from config.yaml
        macro_data: MacroSnapshot from FRED (optional)
        fundamentals_data: Dict of ticker -> StockFundamentals (optional)
    """
    report = MarketHealthReport()

    # Track data source completeness
    report.data_sources_present.append("technical")
    if macro_data and macro_data.indicators:
        report.data_sources_present.append("macro")
    elif macro_data is not None:
        report.data_sources_missing.append("macro (FRED: 0 indicators)")
    else:
        report.data_sources_missing.append("macro (FRED)")
    if fundamentals_data:
        report.data_sources_present.append("fundamental")
    else:
        report.data_sources_missing.append("fundamental (earnings/insider)")

    # Layer 1: Technical signals (lagging)
    _check_vix(market_data, thresholds, report)
    _check_large_drops(market_data, thresholds, report)
    _check_death_crosses(market_data, report)
    _check_rsi_extremes(market_data, thresholds, report)
    _check_breadth_divergence(market_data, report)
    _check_treasury_signals(market_data, report)

    # Layer 2: Macroeconomic signals (leading)
    if macro_data and macro_data.indicators:
        _check_macro_signals(macro_data, report)

    # Layer 3: Fundamental signals (leading)
    if fundamentals_data:
        _check_fundamental_signals(fundamentals_data, report)

    # Layer 4: Market structure (research-validated — CNN F&G, OFR FSI, NFCI)
    _check_breadth_percentage(market_data, report)
    _check_52week_extremes(market_data, report)
    _check_drawdown_from_peak(market_data, report)
    _check_safe_haven_rotation(market_data, report)

    # Meta-signal: convergence amplifier (must run after all other signals)
    _check_signal_convergence(report)

    uncapped, capped, contributions = compute_score_from_signals(report.signals)
    report.score_uncapped = uncapped
    report.score = capped
    report.score_contributions = contributions
    report.overall_risk = _score_to_level(report.score_uncapped)
    report.confidence = _assess_confidence(report)

    return report


def get_position_guidance(risk_level: str) -> dict:
    """Return position sizing guidance based on market risk level."""
    return POSITION_SIZING.get(risk_level, POSITION_SIZING["elevated"])


def get_ticker_position_guidance(risk_score: int, is_short: bool = False) -> str:
    """Return position sizing guidance for a specific opportunity."""
    if is_short:
        return SHORT_POSITION_SIZING
    for max_score, position, stop in POSITION_SIZING_BY_SCORE:
        if risk_score <= max_score:
            return f"{position} | Stop-loss: {stop}"
    return "0.5-1% of portfolio | Stop-loss: 5% below entry"


# --- Layer 1: Technical Checks (Lagging) ---

def _check_vix(market_data: dict, thresholds: dict, report: MarketHealthReport):
    vix_data = _find_ticker(market_data.get("indices", []), "^VIX")
    if not vix_data or vix_data.get("price") is None:
        return

    vix = vix_data["price"]

    if vix >= thresholds.get("vix_crisis", 40):
        report.signals.append(RiskSignal(
            name="VIX Crisis Level", severity="critical", category="volatility",
            message=f"VIX at {vix:.1f} — crisis-level fear. Markets in extreme stress.",
            ticker="^VIX", value=vix,
        ))
    elif vix >= thresholds.get("vix_high", 30):
        report.signals.append(RiskSignal(
            name="VIX High", severity="critical", category="volatility",
            message=f"VIX at {vix:.1f} — high fear. Significant market anxiety.",
            ticker="^VIX", value=vix,
        ))
    elif vix >= thresholds.get("vix_elevated", 20):
        report.signals.append(RiskSignal(
            name="VIX Elevated", severity="warning", category="volatility",
            message=f"VIX at {vix:.1f} — elevated anxiety above normal range.",
            ticker="^VIX", value=vix,
        ))
    else:
        report.signals.append(RiskSignal(
            name="VIX Normal", severity="info", category="volatility",
            message=f"VIX at {vix:.1f} — within normal range.",
            ticker="^VIX", value=vix,
        ))


def _check_large_drops(market_data: dict, thresholds: dict, report: MarketHealthReport):
    daily_threshold = thresholds.get("daily_drop_alert", -3.0)
    weekly_threshold = thresholds.get("weekly_drop_alert", -5.0)
    skip_tickers = {"^VIX"}

    all_assets = []
    for key in ["indices", "stocks", "etfs", "crypto", "forex"]:
        all_assets.extend(market_data.get(key, []))

    for asset in all_assets:
        ticker = asset.get("ticker", "???")
        if ticker in skip_tickers:
            continue

        if asset.get("change_pct_1d") is not None and asset["change_pct_1d"] <= daily_threshold:
            severity = "critical" if asset["change_pct_1d"] <= daily_threshold * 2 else "warning"
            report.signals.append(RiskSignal(
                name="Large Daily Drop", severity=severity, category="volatility",
                message=f"{ticker} dropped {asset['change_pct_1d']:+.2f}% today.",
                ticker=ticker, value=asset["change_pct_1d"],
            ))

        if asset.get("change_pct_1w") is not None and asset["change_pct_1w"] <= weekly_threshold:
            severity = "critical" if asset["change_pct_1w"] <= weekly_threshold * 2 else "warning"
            report.signals.append(RiskSignal(
                name="Large Weekly Drop", severity=severity, category="volatility",
                message=f"{ticker} dropped {asset['change_pct_1w']:+.2f}% this week.",
                ticker=ticker, value=asset["change_pct_1w"],
            ))


def _check_death_crosses(market_data: dict, report: MarketHealthReport):
    """Aggregate death crosses into a single breadth signal (avoids per-ticker score explosion)."""
    watchable = market_data.get("indices", []) + market_data.get("stocks", []) + market_data.get("etfs", [])
    crossed: list[str] = []
    for asset in watchable:
        ma50 = asset.get("fifty_day_ma")
        ma200 = asset.get("two_hundred_day_ma")
        if ma50 is None or ma200 is None:
            continue
        if ma50 < ma200:
            crossed.append(asset.get("ticker", "???"))

    if crossed:
        n = len(crossed)
        sample = ", ".join(crossed[:8])
        more = f" (+{n - 8} more)" if n > 8 else ""
        sev = "critical" if n >= 8 else "warning"
        report.signals.append(RiskSignal(
            name="Death cross breadth",
            severity=sev,
            category="technical",
            message=f"{n} watched assets have 50-day MA below 200-day MA. Names: {sample}{more}.",
            ticker="",
            value=float(n),
        ))


def _check_rsi_extremes(market_data: dict, thresholds: dict, report: MarketHealthReport):
    overbought = thresholds.get("rsi_overbought", 70)
    oversold = thresholds.get("rsi_oversold", 30)
    all_assets = []
    for key in ["indices", "stocks", "etfs"]:
        all_assets.extend(market_data.get(key, []))

    for asset in all_assets:
        rsi = asset.get("rsi_14")
        if rsi is None:
            continue
        ticker = asset.get("ticker", "???")
        if rsi >= overbought:
            report.signals.append(RiskSignal(
                name="RSI Overbought", severity="warning", category="technical",
                message=f"{ticker}: RSI at {rsi:.1f} — overbought, potential pullback ahead.",
                ticker=ticker, value=rsi,
            ))
        elif rsi <= oversold:
            report.signals.append(RiskSignal(
                name="RSI Oversold", severity="info", category="technical",
                message=f"{ticker}: RSI at {rsi:.1f} — oversold, potential bounce opportunity.",
                ticker=ticker, value=rsi,
            ))


def _check_breadth_divergence(market_data: dict, report: MarketHealthReport):
    sp500 = _find_ticker(market_data.get("indices", []), "^GSPC")
    russell = _find_ticker(market_data.get("indices", []), "^RUT")
    if not sp500 or not russell:
        return
    sp_1m = sp500.get("change_pct_1m")
    rut_1m = russell.get("change_pct_1m")
    if sp_1m is None or rut_1m is None:
        return
    divergence = sp_1m - rut_1m
    if divergence > 5:
        report.signals.append(RiskSignal(
            name="Breadth Divergence", severity="warning", category="macro",
            message=f"S&P 500 ({sp_1m:+.1f}% 1M) outperforming Russell 2000 ({rut_1m:+.1f}% 1M) — breadth problem.",
            value=divergence,
        ))


def _check_treasury_signals(market_data: dict, report: MarketHealthReport):
    tnx = _find_ticker(market_data.get("indices", []), "^TNX")
    if not tnx:
        return
    yield_1m_change = tnx.get("change_pct_1m")
    if yield_1m_change is not None and abs(yield_1m_change) > 10:
        direction = "spiking" if yield_1m_change > 0 else "plunging"
        report.signals.append(RiskSignal(
            name="Treasury Yield Volatility", severity="warning", category="macro",
            message=f"10-Year Treasury yield {direction} ({yield_1m_change:+.1f}% monthly change).",
            ticker="^TNX", value=yield_1m_change,
        ))


# --- Layer 2: Macroeconomic Checks (Leading) ---

def _check_macro_signals(macro_data, report: MarketHealthReport):
    """Incorporate FRED macroeconomic signals into risk assessment."""
    for indicator in macro_data.indicators:
        if indicator.signal == "critical":
            report.signals.append(RiskSignal(
                name=f"Macro: {indicator.name}", severity="critical", category="macro",
                message=indicator.description,
                value=indicator.value, signal_type="leading",
            ))
        elif indicator.signal == "warning":
            report.signals.append(RiskSignal(
                name=f"Macro: {indicator.name}", severity="warning", category="macro",
                message=indicator.description,
                value=indicator.value, signal_type="leading",
            ))
        elif indicator.signal == "bearish":
            report.signals.append(RiskSignal(
                name=f"Macro: {indicator.name}", severity="elevated", category="macro",
                message=indicator.description,
                value=indicator.value, signal_type="leading",
            ))

    if macro_data.yield_curve_inverted:
        report.signals.append(RiskSignal(
            name="YIELD CURVE INVERTED", severity="critical", category="macro",
            message="Yield curve is inverted — historically precedes every US recession since 1955. "
                    "This is the single most reliable recession predictor.",
            signal_type="leading",
        ))

    if macro_data.credit_stress:
        report.signals.append(RiskSignal(
            name="CREDIT STRESS", severity="critical", category="macro",
            message="High-yield credit spreads indicate significant corporate distress fears. "
                    "Widening spreads preceded the 2008 crisis by months.",
            signal_type="leading",
        ))


# --- Layer 3: Fundamental Checks (Leading) ---

# Breadth signals use `value` = count; points sublinear in count (see signal_points).
# Caps raised after research-validated recalibration (April 2026) — professional
# breadth models (Schwab, CNN Fear & Greed) give structural breadth much more
# weight relative to per-ticker volatility events.
_MAX_EPS_DISTRESS_BREADTH_POINTS = 40
_MAX_DEATH_CROSS_BREADTH_POINTS = 40


def _check_fundamental_signals(fundamentals_data: dict, report: MarketHealthReport):
    """Aggregate fundamentals into breadth signals (avoids per-ticker score explosion)."""
    deteriorating: list[str] = []
    insider_selling: list[str] = []
    distressed: list[str] = []

    for ticker, fund in fundamentals_data.items():
        if fund.eps_revision_trend == "deteriorating":
            deteriorating.append(ticker)
        if fund.insider_signal == "selling":
            insider_selling.append(ticker)
        if fund.fundamental_health == "distressed":
            distressed.append(ticker)

    if deteriorating:
        n = len(deteriorating)
        sample = ", ".join(deteriorating[:8])
        more = f" (+{n - 8} more)" if n > 8 else ""
        report.signals.append(RiskSignal(
            name="EPS revisions breadth",
            severity="warning",
            category="fundamental",
            message=(
                f"{n} watched stocks have deteriorating analyst EPS estimates (30d). "
                f"Examples: {sample}{more}."
            ),
            ticker="",
            signal_type="leading",
            value=float(n),
        ))

    if insider_selling:
        n = len(insider_selling)
        sample = ", ".join(insider_selling[:8])
        more = f" (+{n - 8} more)" if n > 8 else ""
        sev = "warning" if n >= 3 else "info"
        report.signals.append(RiskSignal(
            name="Insider selling breadth",
            severity=sev,
            category="fundamental",
            message=(
                f"{n} watched stocks show insider selling activity. "
                f"Names: {sample}{more}."
            ),
            ticker="",
            signal_type="leading",
            value=float(n),
        ))

    if distressed:
        n = len(distressed)
        sample = ", ".join(distressed[:8])
        more = f" (+{n - 8} more)" if n > 8 else ""
        report.signals.append(RiskSignal(
            name="Fundamental distress breadth",
            severity="warning",
            category="fundamental",
            message=(
                f"{n} watched stocks flagged as fundamental distress (weak health screen). "
                f"Examples: {sample}{more}."
            ),
            ticker="",
            signal_type="leading",
            value=float(n),
        ))


# --- Layer 4: Market Structure Checks (Research-Validated April 2026) ---
# Based on CNN Fear & Greed Index, OFR Financial Stress Index, Chicago Fed NFCI,
# and academic systemic risk literature.


def _check_breadth_percentage(market_data: dict, report: MarketHealthReport):
    """Percentage of watched assets trading above 200-day MA.

    This is the institutional standard for market breadth (Schwab, StockCharts,
    CNN Fear & Greed "Stock Price Breadth"). Professional thresholds:
      >70%  = healthy bull
      50-70 = neutral
      30-50 = warning (market weakness)
      <30   = critical (very weak)
    """
    watchable = (
        market_data.get("stocks", [])
        + market_data.get("etfs", [])
        + market_data.get("indices", [])
    )
    total = 0
    above = 0
    for asset in watchable:
        ma200 = asset.get("two_hundred_day_ma")
        price = asset.get("price")
        if ma200 is None or price is None:
            continue
        total += 1
        if price > ma200:
            above += 1

    if total < 5:
        return

    pct = above / total * 100
    if pct < 30:
        report.signals.append(RiskSignal(
            name="Breadth: % above 200-day MA",
            severity="critical",
            category="technical",
            message=(
                f"Only {pct:.0f}% of {total} watched assets trade above their "
                f"200-day MA — broad market deterioration."
            ),
            value=pct,
        ))
    elif pct < 50:
        report.signals.append(RiskSignal(
            name="Breadth: % above 200-day MA",
            severity="warning",
            category="technical",
            message=(
                f"{pct:.0f}% of {total} watched assets trade above their "
                f"200-day MA — weakening breadth."
            ),
            value=pct,
        ))


def _check_52week_extremes(market_data: dict, report: MarketHealthReport):
    """CNN Fear & Greed "Stock Price Strength" — ratio of stocks near
    52-week lows vs highs.  A stock is "near low" if it sits in the bottom
    20% of its 52-week range, "near high" if in the top 20%.
    """
    watchable = market_data.get("stocks", []) + market_data.get("etfs", [])
    near_low = 0
    near_high = 0
    counted = 0

    for asset in watchable:
        price = asset.get("price")
        hi = asset.get("fifty_two_week_high")
        lo = asset.get("fifty_two_week_low")
        if price is None or hi is None or lo is None or hi == lo:
            continue
        counted += 1
        position = (price - lo) / (hi - lo)
        if position <= 0.20:
            near_low += 1
        elif position >= 0.80:
            near_high += 1

    if counted < 5:
        return

    pct_near_low = near_low / counted * 100
    if pct_near_low >= 70:
        report.signals.append(RiskSignal(
            name="52-week lows breadth",
            severity="critical",
            category="technical",
            message=(
                f"{near_low}/{counted} watched assets ({pct_near_low:.0f}%) sit "
                f"near 52-week lows vs {near_high} near highs — extreme weakness."
            ),
            value=pct_near_low,
        ))
    elif pct_near_low >= 50:
        report.signals.append(RiskSignal(
            name="52-week lows breadth",
            severity="warning",
            category="technical",
            message=(
                f"{near_low}/{counted} watched assets ({pct_near_low:.0f}%) sit "
                f"near 52-week lows vs {near_high} near highs."
            ),
            value=pct_near_low,
        ))


def _check_drawdown_from_peak(market_data: dict, report: MarketHealthReport):
    """Sustained drawdown from S&P 500 peak — standard portfolio risk metric.

    Uses fiftyTwoWeekHigh from the index data.  This creates a persistent
    floor under the score during sustained declines (unlike per-ticker drops
    that come and go weekly).
    """
    sp500 = _find_ticker(market_data.get("indices", []), "^GSPC")
    if not sp500:
        return

    price = sp500.get("price")
    peak = sp500.get("fifty_two_week_high")
    if price is None or peak is None or peak <= 0:
        return

    drawdown_pct = (price - peak) / peak * 100  # negative value

    if drawdown_pct <= -20:
        report.signals.append(RiskSignal(
            name="S&P 500 drawdown from peak",
            severity="critical",
            category="technical",
            message=(
                f"S&P 500 is {drawdown_pct:.1f}% below its 52-week high — "
                f"bear market territory. Recovery from -50% requires +100% gain."
            ),
            value=drawdown_pct,
        ))
    elif drawdown_pct <= -10:
        report.signals.append(RiskSignal(
            name="S&P 500 drawdown from peak",
            severity="critical",
            category="technical",
            message=(
                f"S&P 500 is {drawdown_pct:.1f}% below its 52-week high — "
                f"correction deepening into bear territory."
            ),
            value=drawdown_pct,
        ))
    elif drawdown_pct <= -5:
        report.signals.append(RiskSignal(
            name="S&P 500 drawdown from peak",
            severity="warning",
            category="technical",
            message=(
                f"S&P 500 is {drawdown_pct:.1f}% below its 52-week high — "
                f"correction territory."
            ),
            value=drawdown_pct,
        ))


def _check_safe_haven_rotation(market_data: dict, report: MarketHealthReport):
    """Safe-haven rotation — validated by CNN Fear & Greed ("Safe Haven Demand")
    and OFR Financial Stress Index ("Safe Assets" category).

    Detects capital fleeing from equities into gold and/or long-term treasuries.
    """
    etfs = market_data.get("etfs", [])
    spy = _find_ticker(etfs, "SPY")
    gld = _find_ticker(etfs, "GLD")
    tlt = _find_ticker(etfs, "TLT")

    if not spy:
        return
    spy_1m = spy.get("change_pct_1m")
    if spy_1m is None:
        return

    gld_fired = False
    if gld:
        gld_1m = gld.get("change_pct_1m")
        if gld_1m is not None and gld_1m > 5 and spy_1m < -2:
            gld_fired = True
            report.signals.append(RiskSignal(
                name="Safe-haven rotation: gold",
                severity="warning",
                category="macro",
                message=(
                    f"Gold (GLD) up {gld_1m:+.1f}% (1M) while equities (SPY) "
                    f"down {spy_1m:+.1f}% — capital fleeing to safety."
                ),
                value=gld_1m,
            ))

    tlt_fired = False
    if tlt:
        tlt_1m = tlt.get("change_pct_1m")
        if tlt_1m is not None and tlt_1m > 3 and spy_1m < -3:
            tlt_fired = True
            report.signals.append(RiskSignal(
                name="Safe-haven rotation: treasuries",
                severity="warning",
                category="macro",
                message=(
                    f"Long-term treasuries (TLT) up {tlt_1m:+.1f}% (1M) while "
                    f"equities (SPY) down {spy_1m:+.1f}% — treasury flight."
                ),
                value=tlt_1m,
            ))

    if gld_fired and tlt_fired:
        report.signals.append(RiskSignal(
            name="Broad safe-haven flight",
            severity="critical",
            category="macro",
            message=(
                "Both gold and long-term treasuries rallying while equities "
                "decline — broad risk-off positioning across asset classes."
            ),
            value=0,
        ))


def _check_signal_convergence(report: MarketHealthReport):
    """Signal convergence amplifier — validated by systemic risk research.

    "Systemic financial stress occurs when multiple individual stress measures
    become extremely high and strongly co-dependent simultaneously."
    — ECB Working Paper 2842

    When many independent signals fire across multiple categories, the
    convergence itself is a risk signal beyond the sum of individual parts.
    """
    serious = [s for s in report.signals if s.severity in ("critical", "warning")]
    n = len(serious)
    categories = {s.category for s in serious}
    n_cats = len(categories)

    if n >= 12:
        pts_label = f"{n} risk signals across {n_cats} categories"
        report.signals.append(RiskSignal(
            name="Signal convergence",
            severity="critical",
            category="technical",
            message=(
                f"Systemic convergence: {pts_label} firing simultaneously — "
                f"historically precedes sustained downturns."
            ),
            value=float(n),
        ))
    elif n >= 8:
        pts_label = f"{n} risk signals across {n_cats} categories"
        report.signals.append(RiskSignal(
            name="Signal convergence",
            severity="warning",
            category="technical",
            message=f"Elevated convergence: {pts_label} firing simultaneously.",
            value=float(n),
        ))

    if n >= 8 and n_cats >= 3:
        report.signals.append(RiskSignal(
            name="Cross-category convergence",
            severity="info",
            category="technical",
            message=(
                f"Risk signals span {n_cats} categories ({', '.join(sorted(categories))}) "
                f"— cross-domain stress amplification."
            ),
            value=float(n_cats),
        ))


# --- Confidence & Scoring ---

def _assess_confidence(report: MarketHealthReport) -> str:
    """Assess confidence in the risk assessment based on data completeness and signal agreement."""
    source_count = len(report.data_sources_present)
    leading_count = report.leading_signal_count

    if source_count >= 3 and leading_count >= 2:
        return "high"
    elif source_count >= 2 or leading_count >= 1:
        return "medium"
    return "low"


def _find_ticker(items: list[dict], ticker: str) -> dict | None:
    for item in items:
        if item.get("ticker") == ticker:
            return item
    return None


def _breadth_points(n: int, cap: int = _MAX_EPS_DISTRESS_BREADTH_POINTS) -> int:
    """Sublinear points from breadth count n (leading-warning baseline ≈15 at n=1).

    Bump multiplier raised from 5→8 after research recalibration so that breadth
    beyond the first occurrence carries more weight (aligns with institutional
    breadth models where widespread deterioration is qualitatively different).
    """
    if n <= 0:
        return 0
    base_weight = 1.5  # leading
    linear = int(10 * base_weight)  # 15 for first bucket (warning tier)
    bump = int(8 * base_weight * math.log1p(max(0, n - 1)) / math.log1p(25))
    return min(linear + bump, cap)


def _lagging_breadth_points(n: int, cap: int = _MAX_DEATH_CROSS_BREADTH_POINTS) -> int:
    """Sublinear points for lagging breadth signals (base weight 1.0, not 1.5).

    Bump multiplier raised from 5→8 to better reflect that broad death crosses
    are a structural signal, not per-ticker noise.
    """
    if n <= 0:
        return 0
    linear = 10  # warning tier × 1.0 lagging weight
    bump = int(8 * math.log1p(max(0, n - 1)) / math.log1p(25))
    return min(linear + bump, cap)


def signal_points(signal: RiskSignal) -> int:
    """Points this signal adds to the uncapped risk sum (breadth signals scaled)."""
    if signal.name in ("EPS revisions breadth", "Fundamental distress breadth"):
        return _breadth_points(int(signal.value) if signal.value else 1)

    if signal.name == "Insider selling breadth":
        n = int(signal.value) if signal.value else 1
        if signal.severity == "warning":
            return _breadth_points(n, cap=_MAX_EPS_DISTRESS_BREADTH_POINTS)
        return 2  # info tier, leading but cap at base info rule

    if signal.name == "Death cross breadth":
        n = int(signal.value) if signal.value else 1
        return _lagging_breadth_points(n)

    weight = 1.5 if signal.signal_type == "leading" else 1.0
    if signal.severity == "critical":
        return int(25 * weight)
    if signal.severity == "warning":
        return int(10 * weight)
    if signal.severity == "elevated":
        return int(5 * weight)
    if signal.severity == "info":
        return 2
    return 0


def compute_score_from_signals(signals: list[RiskSignal]) -> tuple[int, int, list[ScoreContribution]]:
    """Return (uncapped_total, capped_score, contributions sorted by points descending)."""
    contributions: list[ScoreContribution] = []
    total = 0
    for signal in signals:
        pts = signal_points(signal)
        total += pts
        contributions.append(
            ScoreContribution(
                name=signal.name,
                severity=signal.severity,
                category=signal.category,
                signal_type=signal.signal_type,
                ticker=signal.ticker or "—",
                points=pts,
            )
        )
    contributions.sort(key=lambda c: (-c.points, c.category, c.name))
    capped = min(total, 100)
    return total, capped, contributions


def score_macro_layer_only(macro_data) -> tuple[int, int, list[ScoreContribution]]:
    """Macro + derived flags only — for historical replay (no technicals/fundamentals)."""
    report = MarketHealthReport()
    _check_macro_signals(macro_data, report)
    return compute_score_from_signals(report.signals)


def _score_to_level(score: int) -> str:
    """Map uncapped risk score to graduated severity level.

    The scale extends beyond 100 to differentiate within crisis conditions.
    Levels above 'critical' use the uncapped score so worsening conditions
    are visible even after the 0-100 display cap.
    """
    if score >= 200:
        return "heavy_stress"
    if score >= 150:
        return "severe_stress"
    if score >= 100:
        return "compounding_stress"
    if score >= 80:
        return "acute_stress"
    if score >= 60:
        return "high"
    if score >= 40:
        return "elevated"
    if score >= 20:
        return "moderate"
    return "low"
