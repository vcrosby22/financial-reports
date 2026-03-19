"""Fetch macroeconomic indicators from FRED (Federal Reserve Economic Data).

These are the leading indicators that precede recessions and market collapses.
Free API — requires a key from https://fred.stlouisfed.org/docs/api/api_key.html
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from rich.console import Console

from ..config import get_settings

console = Console()

FRED_SERIES = {
    "T10Y2Y": "10Y-2Y Yield Spread",
    "T10Y3M": "10Y-3M Yield Spread",
    "BAMLH0A0HYM2": "High Yield Credit Spread",
    "ICSA": "Initial Unemployment Claims",
    "UMCSENT": "Consumer Confidence (UMich)",
    "FEDFUNDS": "Fed Funds Rate",
    "M2SL": "M2 Money Supply",
}


@dataclass
class MacroIndicator:
    series_id: str
    name: str
    value: float
    previous_value: float | None = None
    change: float | None = None
    signal: str = "neutral"  # bullish, bearish, neutral, warning, critical
    description: str = ""


@dataclass
class MacroSnapshot:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    indicators: list[MacroIndicator] = field(default_factory=list)
    yield_curve_inverted: bool = False
    credit_stress: bool = False
    recession_signals: int = 0

    def to_prompt_text(self) -> str:
        """Format macro data for inclusion in Claude prompts."""
        lines = ["=== MACROECONOMIC INDICATORS (FRED) ==="]
        for ind in self.indicators:
            line = f"{ind.name}: {ind.value:.2f}"
            if ind.change is not None:
                line += f" (change: {ind.change:+.2f})"
            line += f" | Signal: {ind.signal.upper()}"
            if ind.description:
                line += f" — {ind.description}"
            lines.append(line)

        lines.append(f"\nYield Curve Inverted: {'YES' if self.yield_curve_inverted else 'NO'}")
        lines.append(f"Credit Stress Detected: {'YES' if self.credit_stress else 'NO'}")
        lines.append(f"Recession Signals Active: {self.recession_signals}")
        return "\n".join(lines)


def fetch_macro_data() -> MacroSnapshot | None:
    """Fetch all tracked FRED series and assess macro conditions."""
    settings = get_settings()
    if not settings.fred_api_key:
        console.print("[yellow]No FRED_API_KEY configured — skipping macro data.[/yellow]")
        return None

    try:
        from fredapi import Fred
        fred = Fred(api_key=settings.fred_api_key)
    except Exception as e:
        console.print(f"[red]Failed to connect to FRED: {e}[/red]")
        return None

    snapshot = MacroSnapshot()

    for series_id, name in FRED_SERIES.items():
        console.print(f"  Fetching {name} ({series_id})...", style="dim")
        indicator = _fetch_single_series(fred, series_id, name)
        if indicator:
            snapshot.indicators.append(indicator)

    _assess_yield_curve(snapshot)
    _assess_credit_conditions(snapshot)
    _count_recession_signals(snapshot)

    return snapshot


def _fetch_single_series(fred, series_id: str, name: str) -> MacroIndicator | None:
    try:
        end = datetime.now()
        start = end - timedelta(days=365)
        data = fred.get_series(series_id, observation_start=start, observation_end=end)

        if data is None or data.empty:
            return None

        data = data.dropna()
        if data.empty:
            return None

        current = float(data.iloc[-1])
        previous = float(data.iloc[-2]) if len(data) > 1 else None
        change = current - previous if previous is not None else None

        indicator = MacroIndicator(
            series_id=series_id,
            name=name,
            value=current,
            previous_value=previous,
            change=change,
        )

        _classify_signal(indicator)
        return indicator

    except Exception as e:
        console.print(f"[red]Error fetching {series_id}: {e}[/red]")
        return None


def _classify_signal(indicator: MacroIndicator):
    """Assign a signal classification based on the indicator value and direction."""
    sid = indicator.series_id

    if sid == "T10Y2Y":
        if indicator.value < 0:
            indicator.signal = "critical"
            indicator.description = "Yield curve INVERTED — historically precedes recessions within 6-18 months"
        elif indicator.value < 0.5:
            indicator.signal = "warning"
            indicator.description = "Yield curve nearly flat — watch for inversion"
        else:
            indicator.signal = "neutral"
            indicator.description = "Normal upward-sloping yield curve"

    elif sid == "T10Y3M":
        if indicator.value < 0:
            indicator.signal = "critical"
            indicator.description = "10Y-3M inverted — most reliable recession predictor (preceded every recession since 1955)"
        elif indicator.value < 0.5:
            indicator.signal = "warning"
            indicator.description = "10Y-3M spread dangerously narrow"
        else:
            indicator.signal = "neutral"
            indicator.description = "Healthy spread between short and long-term rates"

    elif sid == "BAMLH0A0HYM2":
        if indicator.value > 6:
            indicator.signal = "critical"
            indicator.description = "Credit spreads at crisis levels — severe corporate distress feared"
        elif indicator.value > 4.5:
            indicator.signal = "warning"
            indicator.description = "Credit spreads elevated — rising fear of corporate defaults"
        elif indicator.value > 3.5:
            indicator.signal = "bearish"
            indicator.description = "Credit spreads above average — some market stress"
        else:
            indicator.signal = "neutral"
            indicator.description = "Credit markets calm"

    elif sid == "ICSA":
        if indicator.change is not None and indicator.change > 30000:
            indicator.signal = "warning"
            indicator.description = f"Unemployment claims rising sharply (+{indicator.change:,.0f}) — labor market weakening"
        elif indicator.value > 300000:
            indicator.signal = "bearish"
            indicator.description = "Elevated unemployment claims — economy showing stress"
        else:
            indicator.signal = "neutral"
            indicator.description = "Labor market stable"

    elif sid == "UMCSENT":
        if indicator.value < 60:
            indicator.signal = "critical"
            indicator.description = "Consumer confidence at recessionary levels"
        elif indicator.value < 70:
            indicator.signal = "warning"
            indicator.description = "Consumer confidence low — spending likely to contract"
        elif indicator.change is not None and indicator.change < -5:
            indicator.signal = "bearish"
            indicator.description = f"Consumer confidence dropping sharply ({indicator.change:+.1f})"
        else:
            indicator.signal = "neutral"
            indicator.description = "Consumer sentiment within normal range"

    elif sid == "FEDFUNDS":
        if indicator.change is not None and indicator.change > 0:
            indicator.signal = "bearish"
            indicator.description = "Fed raising rates — tightening monetary policy"
        elif indicator.change is not None and indicator.change < 0:
            indicator.signal = "bullish"
            indicator.description = "Fed cutting rates — easing monetary policy"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Fed funds rate holding at {indicator.value:.2f}%"

    elif sid == "M2SL":
        if indicator.change is not None and indicator.change < 0:
            indicator.signal = "warning"
            indicator.description = "Money supply contracting — liquidity tightening"
        else:
            indicator.signal = "neutral"
            indicator.description = "Money supply stable or growing"


def _assess_yield_curve(snapshot: MacroSnapshot):
    for ind in snapshot.indicators:
        if ind.series_id in ("T10Y2Y", "T10Y3M") and ind.value < 0:
            snapshot.yield_curve_inverted = True
            break


def _assess_credit_conditions(snapshot: MacroSnapshot):
    for ind in snapshot.indicators:
        if ind.series_id == "BAMLH0A0HYM2" and ind.value > 4.5:
            snapshot.credit_stress = True
            break


def _count_recession_signals(snapshot: MacroSnapshot):
    count = 0
    for ind in snapshot.indicators:
        if ind.signal in ("critical", "warning"):
            count += 1
    snapshot.recession_signals = count
