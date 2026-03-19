"""Opportunity screening engine — surfaces risk-adjusted investment opportunities.

Combines technical, macro, and fundamental data to identify:
  - Long opportunities (buy) across short/medium/long time horizons
  - Short opportunities (sell) for deteriorating assets

Every opportunity carries: risk score, confidence, thesis,
what could go wrong, and position sizing. No opportunity is
presented without its risks.
"""

from dataclasses import dataclass, field

from ..data.fundamentals import StockFundamentals
from .risk import MarketHealthReport, get_ticker_position_guidance


@dataclass
class Opportunity:
    ticker: str
    name: str
    direction: str  # long, short
    time_horizon: str  # short_term, medium_term, long_term
    risk_score: int  # 1-10
    confidence: str  # low, medium, high
    thesis: str
    risks: str  # what could go wrong
    position_sizing: str
    signals_for: list[str] = field(default_factory=list)
    signals_against: list[str] = field(default_factory=list)

    @property
    def risk_label(self) -> str:
        if self.risk_score <= 3:
            return "Low"
        elif self.risk_score <= 6:
            return "Medium"
        return "High"

    @property
    def horizon_label(self) -> str:
        return {
            "short_term": "Short-term (1–4 weeks)",
            "medium_term": "Medium-term (1–3 months)",
            "long_term": "Long-term (3–12 months)",
        }.get(self.time_horizon, self.time_horizon)


def screen_opportunities(
    market_data: dict,
    fundamentals: dict[str, StockFundamentals],
    macro_data,
    health: MarketHealthReport,
) -> list[Opportunity]:
    """Screen all watched assets for long and short opportunities."""
    opportunities = []

    macro_supportive = _is_macro_supportive(macro_data)
    macro_adverse = _is_macro_adverse(macro_data)
    market_risk = health.overall_risk

    stock_items = {item["ticker"]: item for item in market_data.get("stocks", [])}
    etf_items = {item["ticker"]: item for item in market_data.get("etfs", [])}
    all_equities = {**stock_items, **etf_items}

    for ticker, item in all_equities.items():
        fund = fundamentals.get(ticker)
        name = item.get("name", ticker)

        longs = _screen_long(ticker, name, item, fund, macro_supportive, macro_adverse, market_risk)
        opportunities.extend(longs)

        shorts = _screen_short(ticker, name, item, fund, macro_adverse, market_risk)
        opportunities.extend(shorts)

    opportunities.sort(key=lambda o: (
        {"high": 2, "medium": 1, "low": 0}.get(o.confidence, 0),
        -o.risk_score,
    ), reverse=True)

    return opportunities


def _screen_long(
    ticker: str,
    name: str,
    item: dict,
    fund: StockFundamentals | None,
    macro_supportive: bool,
    macro_adverse: bool,
    market_risk: str,
) -> list[Opportunity]:
    """Screen a single asset for long (buy) opportunities."""
    results = []

    rsi = item.get("rsi_14")
    ma50 = item.get("fifty_day_ma")
    ma200 = item.get("two_hundred_day_ma")
    change_1m = item.get("change_pct_1m")
    price = item.get("price")

    is_oversold = rsi is not None and rsi <= 30
    is_golden_cross = ma50 is not None and ma200 is not None and ma50 > ma200
    has_death_cross = ma50 is not None and ma200 is not None and ma50 < ma200
    is_down_big = change_1m is not None and change_1m <= -10

    has_strong_fundamentals = fund is not None and fund.fundamental_health in ("strong", "moderate")
    has_improving_eps = fund is not None and fund.eps_revision_trend == "improving"
    has_deteriorating_eps = fund is not None and fund.eps_revision_trend == "deteriorating"
    has_insider_buying = fund is not None and fund.insider_signal == "buying"
    has_insider_selling = fund is not None and fund.insider_signal == "selling"
    has_analyst_upside = fund is not None and fund.upside_to_mean_target is not None and fund.upside_to_mean_target > 15

    # --- Short-term long: oversold bounce ---
    if is_oversold and not has_deteriorating_eps:
        signals_for = ["RSI oversold (≤30)"]
        signals_against = []
        risk = 5  # base: medium risk for technical plays

        if has_strong_fundamentals:
            signals_for.append(f"Fundamentals: {fund.fundamental_health}")
            risk -= 1
        if macro_supportive:
            signals_for.append("Macro environment supportive")
            risk -= 1
        if is_golden_cross:
            signals_for.append("Golden cross (bullish long-term trend)")
            risk -= 1
        if has_death_cross:
            signals_against.append("Death cross active (bearish long-term trend)")
            risk += 1
        if macro_adverse:
            signals_against.append("Macro headwinds (recession signals)")
            risk += 1
        if has_insider_selling:
            signals_against.append("Insider selling detected")
            risk += 1
        if market_risk in ("high", "critical"):
            signals_against.append(f"Overall market risk: {market_risk}")
            risk += 1

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="long",
            time_horizon="short_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} is technically oversold (RSI {rsi:.0f}). "
                   f"{'Strong' if has_strong_fundamentals else 'Unknown'} fundamentals suggest this is a "
                   f"pullback in a viable business, not a fundamental breakdown. "
                   f"Mean reversion bounce is probable within 1–4 weeks.",
            risks=_build_risk_text(ticker, signals_against, "long", "short_term"),
            position_sizing=get_ticker_position_guidance(risk),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    # --- Medium-term long: improving earnings + analyst upside ---
    if has_improving_eps and has_analyst_upside:
        signals_for = [
            f"EPS revisions improving (up: {fund.eps_revision_up_30d}, down: {fund.eps_revision_down_30d} in 30d)",
            f"Analyst upside: {fund.upside_to_mean_target:+.1f}% to mean target",
        ]
        signals_against = []
        risk = 4

        if has_strong_fundamentals:
            signals_for.append(f"Fundamentals: {fund.fundamental_health}")
            risk -= 1
        if has_insider_buying:
            signals_for.append("Insiders are buying")
            risk -= 1
        if macro_supportive:
            signals_for.append("Macro environment supportive")
            risk -= 1
        if has_death_cross:
            signals_against.append("Death cross active")
            risk += 1
        if macro_adverse:
            signals_against.append("Macro headwinds")
            risk += 1
        if is_oversold:
            signals_for.append("Also technically oversold — dual signal")

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="long",
            time_horizon="medium_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} has analysts revising earnings estimates UPWARD while the stock trades "
                   f"{fund.upside_to_mean_target:+.1f}% below mean analyst target. "
                   f"Improving earnings are the strongest leading indicator of price appreciation "
                   f"over 1–3 months.",
            risks=_build_risk_text(ticker, signals_against, "long", "medium_term"),
            position_sizing=get_ticker_position_guidance(risk),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    # --- Long-term long: quality at a discount ---
    if has_strong_fundamentals and is_down_big and not has_deteriorating_eps:
        signals_for = [
            f"Down {change_1m:+.1f}% in 1 month — significant discount",
            f"Fundamentals: {fund.fundamental_health}",
        ]
        signals_against = []
        risk = 4

        if has_improving_eps:
            signals_for.append("EPS revisions improving")
            risk -= 1
        if has_analyst_upside:
            signals_for.append(f"Analyst upside: {fund.upside_to_mean_target:+.1f}%")
            risk -= 1
        if has_insider_buying:
            signals_for.append("Insiders buying at these levels")
            risk -= 1
        if macro_supportive:
            signals_for.append("Macro environment supportive")
            risk -= 1
        if fund.roe is not None and fund.roe > 0.15:
            signals_for.append(f"ROE {fund.roe:.1%} — strong capital efficiency")
        if fund.debt_to_equity is not None and fund.debt_to_equity < 0.5:
            signals_for.append(f"D/E {fund.debt_to_equity:.2f} — conservative balance sheet")
        if has_death_cross:
            signals_against.append("Death cross — downtrend may continue")
            risk += 1
        if macro_adverse:
            signals_against.append("Macro headwinds — recovery may take longer")
            risk += 1
        if has_insider_selling:
            signals_against.append("Insider selling despite low price")
            risk += 2

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="long",
            time_horizon="long_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} is a {fund.fundamental_health}-quality business trading at a "
                   f"{abs(change_1m):.0f}% discount from its recent high. "
                   f"Buffett's principle: buy quality when others are fearful. "
                   f"Earnings are {'improving' if has_improving_eps else 'stable'}, "
                   f"suggesting the price drop is sentiment-driven, not fundamental.",
            risks=_build_risk_text(ticker, signals_against, "long", "long_term"),
            position_sizing=get_ticker_position_guidance(risk),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    # --- Insider buying cluster (any timeframe) ---
    if has_insider_buying and has_strong_fundamentals and not any(
        o.ticker == ticker and o.time_horizon == "medium_term" for o in results
    ):
        signals_for = [
            f"Insiders buying ({fund.insider_buy_count} buys vs {fund.insider_sell_count} sells)",
            f"Fundamentals: {fund.fundamental_health}",
        ]
        signals_against = []
        risk = 4

        if has_improving_eps:
            signals_for.append("EPS revisions also improving")
            risk -= 1
        if macro_supportive:
            risk -= 1
        if has_death_cross:
            signals_against.append("Death cross active")
            risk += 1
        if macro_adverse:
            signals_against.append("Macro headwinds")
            risk += 1

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="long",
            time_horizon="medium_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} insiders are buying their own stock — those closest to the business "
                   f"are putting their own money in. Combined with {fund.fundamental_health} "
                   f"fundamentals, this is a positive signal over 3–12 months.",
            risks=_build_risk_text(ticker, signals_against, "long", "medium_term"),
            position_sizing=get_ticker_position_guidance(risk),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    return results


def _screen_short(
    ticker: str,
    name: str,
    item: dict,
    fund: StockFundamentals | None,
    macro_adverse: bool,
    market_risk: str,
) -> list[Opportunity]:
    """Screen a single asset for short (sell/short) opportunities."""
    results = []

    rsi = item.get("rsi_14")
    ma50 = item.get("fifty_day_ma")
    ma200 = item.get("two_hundred_day_ma")
    price = item.get("price")

    is_overbought = rsi is not None and rsi >= 70
    has_death_cross = ma50 is not None and ma200 is not None and ma50 < ma200

    has_deteriorating_eps = fund is not None and fund.eps_revision_trend == "deteriorating"
    has_insider_selling = fund is not None and fund.insider_signal == "selling"
    has_weak_fundamentals = fund is not None and fund.fundamental_health in ("weak", "distressed")
    above_analyst_high = (
        fund is not None
        and fund.analyst_target_high is not None
        and price is not None
        and price > fund.analyst_target_high
    )

    # --- Short: overbought + deteriorating fundamentals ---
    if is_overbought and has_deteriorating_eps:
        signals_for = [
            f"RSI overbought ({rsi:.0f})",
            "EPS revisions declining — earnings deteriorating",
        ]
        signals_against = ["Short selling carries unlimited loss risk"]
        risk = 7

        if has_weak_fundamentals:
            signals_for.append(f"Fundamentals: {fund.fundamental_health}")
            risk -= 1
        if has_insider_selling:
            signals_for.append("Insiders also selling")
            risk -= 1
        if macro_adverse:
            signals_for.append("Macro headwinds support bearish thesis")
            risk -= 1
        if not has_death_cross:
            signals_against.append("No death cross — long-term trend still intact")
            risk += 1

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="short",
            time_horizon="short_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} is technically overbought (RSI {rsi:.0f}) while earnings estimates "
                   f"are being revised downward. The market hasn't priced in the deterioration yet. "
                   f"Mean reversion + negative earnings surprises could drive correction.",
            risks=_build_risk_text(ticker, signals_against, "short", "short_term"),
            position_sizing=get_ticker_position_guidance(risk, is_short=True),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    # --- Short: death cross + declining fundamentals + insider selling ---
    if has_death_cross and has_deteriorating_eps and has_insider_selling:
        signals_for = [
            "Death cross (bearish trend confirmed)",
            "EPS revisions declining",
            f"Insiders selling ({fund.insider_sell_count} sales)",
        ]
        signals_against = ["Short selling carries unlimited loss risk"]
        risk = 6

        if has_weak_fundamentals:
            signals_for.append(f"Fundamentals: {fund.fundamental_health}")
            risk -= 1
        if macro_adverse:
            signals_for.append("Macro environment also bearish")
            risk -= 1

        risk = max(1, min(10, risk))
        confidence = _calc_confidence(signals_for, signals_against, fund)

        results.append(Opportunity(
            ticker=ticker,
            name=name,
            direction="short",
            time_horizon="medium_term",
            risk_score=risk,
            confidence=confidence,
            thesis=f"{ticker} has a triple-bearish alignment: death cross (technical), "
                   f"declining earnings revisions (fundamental), and insider selling (those who know "
                   f"the business best are exiting). This convergence is significant.",
            risks=_build_risk_text(ticker, signals_against, "short", "medium_term"),
            position_sizing=get_ticker_position_guidance(risk, is_short=True),
            signals_for=signals_for,
            signals_against=signals_against,
        ))

    # --- Short: trading above analyst high target ---
    if above_analyst_high and not has_deteriorating_eps:
        overvaluation = ((price - fund.analyst_target_high) / fund.analyst_target_high) * 100
        if overvaluation > 5:
            signals_for = [
                f"Price ${price:.2f} is {overvaluation:.1f}% above analyst HIGH target ${fund.analyst_target_high:.2f}",
                "Even the most optimistic analysts think it's overvalued",
            ]
            signals_against = [
                "Short selling carries unlimited loss risk",
                "Momentum can push overbought stocks higher for longer than expected",
            ]
            risk = 8

            if has_insider_selling:
                signals_for.append("Insiders selling at these levels")
                risk -= 1
            if is_overbought:
                signals_for.append(f"Also technically overbought (RSI {rsi:.0f})")
                risk -= 1

            risk = max(1, min(10, risk))
            confidence = _calc_confidence(signals_for, signals_against, fund)

            results.append(Opportunity(
                ticker=ticker,
                name=name,
                direction="short",
                time_horizon="short_term",
                risk_score=risk,
                confidence=confidence,
                thesis=f"{ticker} at ${price:.2f} is trading {overvaluation:.1f}% above even the "
                       f"most optimistic analyst target (${fund.analyst_target_high:.2f}). "
                       f"This level of overvaluation tends to correct — the question is timing.",
                risks=_build_risk_text(ticker, signals_against, "short", "short_term"),
                position_sizing=get_ticker_position_guidance(risk, is_short=True),
                signals_for=signals_for,
                signals_against=signals_against,
            ))

    return results


# --- Helpers ---

def _is_macro_supportive(macro_data) -> bool:
    if macro_data is None:
        return False
    return (
        not macro_data.yield_curve_inverted
        and not macro_data.credit_stress
        and macro_data.recession_signals <= 1
    )


def _is_macro_adverse(macro_data) -> bool:
    if macro_data is None:
        return False
    return macro_data.yield_curve_inverted or macro_data.credit_stress or macro_data.recession_signals >= 3


def _calc_confidence(
    signals_for: list[str],
    signals_against: list[str],
    fund: StockFundamentals | None,
) -> str:
    score = len(signals_for)
    if fund and fund.data_completeness > 0.6:
        score += 1
    score -= len(signals_against)

    if score >= 4:
        return "high"
    elif score >= 2:
        return "medium"
    return "low"


def _build_risk_text(
    ticker: str,
    signals_against: list[str],
    direction: str,
    horizon: str,
) -> str:
    parts = []

    if signals_against:
        parts.extend(signals_against)

    if direction == "short":
        parts.append("Short positions have theoretically unlimited loss — always use a buy-stop")
    if direction == "long" and horizon == "short_term":
        parts.append("Oversold assets can stay oversold — the bounce may not come or may be weak")
    if direction == "long" and horizon == "long_term":
        parts.append("\"Cheap\" stocks can get cheaper — time horizon may extend beyond 12 months")

    if not parts:
        parts.append("No specific contra-signals detected, but all investments carry risk of loss")

    return ". ".join(parts) + "."
