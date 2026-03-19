"""Fetch fundamental data for individual stocks: earnings revisions, insider activity, analyst targets.

These are LEADING indicators that complement the lagging technical indicators
already in the system. Earnings revision trends are the single most powerful
predictor of individual stock direction over 1-3 months.
"""

from dataclasses import dataclass, field

import yfinance as yf
from rich.console import Console

console = Console()


@dataclass
class StockFundamentals:
    ticker: str
    eps_revision_up_7d: int = 0
    eps_revision_down_7d: int = 0
    eps_revision_up_30d: int = 0
    eps_revision_down_30d: int = 0
    eps_revision_trend: str = "neutral"  # improving, deteriorating, neutral

    analyst_target_low: float | None = None
    analyst_target_mean: float | None = None
    analyst_target_high: float | None = None
    current_price: float | None = None
    upside_to_mean_target: float | None = None

    insider_buy_count: int = 0
    insider_sell_count: int = 0
    insider_signal: str = "neutral"  # buying, selling, neutral

    debt_to_equity: float | None = None
    free_cash_flow: float | None = None
    roe: float | None = None
    revenue_growth: float | None = None
    earnings_growth: float | None = None

    # Distress screening scores
    fundamental_health: str = "unknown"  # strong, moderate, weak, distressed

    @property
    def data_completeness(self) -> float:
        """Percentage of data fields that are populated (0.0 to 1.0)."""
        fields = [
            self.eps_revision_trend != "neutral",
            self.analyst_target_mean is not None,
            self.insider_signal != "neutral",
            self.debt_to_equity is not None,
            self.free_cash_flow is not None,
            self.roe is not None,
            self.revenue_growth is not None,
        ]
        return sum(1 for f in fields if f) / len(fields)


def fetch_fundamentals(symbol: str) -> StockFundamentals | None:
    """Fetch comprehensive fundamental data for a single ticker."""
    try:
        ticker = yf.Ticker(symbol)
        fundamentals = StockFundamentals(ticker=symbol)

        _fetch_eps_revisions(ticker, fundamentals)
        _fetch_analyst_targets(ticker, fundamentals)
        _fetch_insider_activity(ticker, fundamentals)
        _fetch_financial_health(ticker, fundamentals)
        _assess_overall_health(fundamentals)

        return fundamentals
    except Exception as e:
        console.print(f"[red]Error fetching fundamentals for {symbol}: {e}[/red]")
        return None


def fetch_fundamentals_batch(symbols: list[str]) -> dict[str, StockFundamentals]:
    """Fetch fundamentals for multiple tickers."""
    results = {}
    for symbol in symbols:
        console.print(f"  Fundamentals for {symbol}...", style="dim")
        data = fetch_fundamentals(symbol)
        if data:
            results[symbol] = data
    return results


def _fetch_eps_revisions(ticker: yf.Ticker, fundamentals: StockFundamentals):
    try:
        revisions = ticker.get_eps_revisions()
        if revisions is not None and not revisions.empty:
            if "upLast7days" in revisions.columns:
                row = revisions.iloc[0] if len(revisions) > 0 else None
                if row is not None:
                    fundamentals.eps_revision_up_7d = int(row.get("upLast7days", 0) or 0)
                    fundamentals.eps_revision_down_7d = int(row.get("downLast7days", 0) or 0)
                    fundamentals.eps_revision_up_30d = int(row.get("upLast30days", 0) or 0)
                    fundamentals.eps_revision_down_30d = int(row.get("downLast30days", 0) or 0)

            net_30d = fundamentals.eps_revision_up_30d - fundamentals.eps_revision_down_30d
            if net_30d > 1:
                fundamentals.eps_revision_trend = "improving"
            elif net_30d < -1:
                fundamentals.eps_revision_trend = "deteriorating"
    except Exception:
        pass


def _fetch_analyst_targets(ticker: yf.Ticker, fundamentals: StockFundamentals):
    try:
        targets = ticker.get_analyst_price_targets()
        if targets is not None:
            if isinstance(targets, dict):
                fundamentals.analyst_target_low = targets.get("low")
                fundamentals.analyst_target_mean = targets.get("mean")
                fundamentals.analyst_target_high = targets.get("high")
                fundamentals.current_price = targets.get("current")
            elif hasattr(targets, "iloc"):
                fundamentals.analyst_target_low = getattr(targets, "low", None)
                fundamentals.analyst_target_mean = getattr(targets, "mean", None)
                fundamentals.analyst_target_high = getattr(targets, "high", None)

        if fundamentals.analyst_target_mean and fundamentals.current_price:
            fundamentals.upside_to_mean_target = (
                (fundamentals.analyst_target_mean - fundamentals.current_price)
                / fundamentals.current_price * 100
            )
    except Exception:
        pass


def _fetch_insider_activity(ticker: yf.Ticker, fundamentals: StockFundamentals):
    try:
        transactions = ticker.insider_transactions
        if transactions is not None and not transactions.empty:
            recent = transactions.head(20)
            text_col = None
            for col in recent.columns:
                if "text" in col.lower() or "transaction" in col.lower():
                    text_col = col
                    break

            if text_col:
                for _, row in recent.iterrows():
                    text = str(row.get(text_col, "")).lower()
                    if "purchase" in text or "buy" in text:
                        fundamentals.insider_buy_count += 1
                    elif "sale" in text or "sell" in text:
                        fundamentals.insider_sell_count += 1

            if fundamentals.insider_buy_count > fundamentals.insider_sell_count + 2:
                fundamentals.insider_signal = "buying"
            elif fundamentals.insider_sell_count > fundamentals.insider_buy_count + 2:
                fundamentals.insider_signal = "selling"
    except Exception:
        pass


def _fetch_financial_health(ticker: yf.Ticker, fundamentals: StockFundamentals):
    try:
        info = ticker.info or {}
        fundamentals.debt_to_equity = info.get("debtToEquity")
        if fundamentals.debt_to_equity:
            fundamentals.debt_to_equity = fundamentals.debt_to_equity / 100  # yfinance returns as percentage

        fundamentals.free_cash_flow = info.get("freeCashflow")
        fundamentals.roe = info.get("returnOnEquity")
        fundamentals.revenue_growth = info.get("revenueGrowth")
        fundamentals.earnings_growth = info.get("earningsGrowth")
    except Exception:
        pass


def _assess_overall_health(fundamentals: StockFundamentals):
    """Classify fundamental health as strong/moderate/weak/distressed."""
    score = 0
    checks = 0

    if fundamentals.roe is not None:
        checks += 1
        if fundamentals.roe > 0.15:
            score += 2
        elif fundamentals.roe > 0.05:
            score += 1
        elif fundamentals.roe < 0:
            score -= 1

    if fundamentals.debt_to_equity is not None:
        checks += 1
        if fundamentals.debt_to_equity < 0.5:
            score += 2
        elif fundamentals.debt_to_equity < 1.0:
            score += 1
        elif fundamentals.debt_to_equity > 2.0:
            score -= 1

    if fundamentals.free_cash_flow is not None:
        checks += 1
        if fundamentals.free_cash_flow > 0:
            score += 1
        else:
            score -= 1

    if fundamentals.eps_revision_trend == "improving":
        score += 1
    elif fundamentals.eps_revision_trend == "deteriorating":
        score -= 1

    if fundamentals.revenue_growth is not None:
        checks += 1
        if fundamentals.revenue_growth > 0.1:
            score += 1
        elif fundamentals.revenue_growth < -0.05:
            score -= 1

    if checks == 0:
        fundamentals.fundamental_health = "unknown"
    elif score >= 4:
        fundamentals.fundamental_health = "strong"
    elif score >= 2:
        fundamentals.fundamental_health = "moderate"
    elif score >= 0:
        fundamentals.fundamental_health = "weak"
    else:
        fundamentals.fundamental_health = "distressed"
