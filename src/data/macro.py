"""Fetch macroeconomic indicators from FRED (Federal Reserve Economic Data).

These are the leading indicators that precede recessions and market collapses.
Free API — requires a key from https://fred.stlouisfed.org/docs/api/api_key.html
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from rich.console import Console

from ..config import get_settings

console = Console()

# (series_id, display_name, category). All FRED official; categories label the HTML report.
# banking_system = system-wide aggregates only (not institution-level CAMELS).
FRED_SERIES: list[tuple[str, str, str]] = [
    ("T10Y2Y", "10Y-2Y Yield Spread", "core_macro"),
    ("T10Y3M", "10Y-3M Yield Spread", "core_macro"),
    ("BAMLH0A0HYM2", "High Yield Credit Spread", "core_macro"),
    ("ICSA", "Initial Unemployment Claims", "core_macro"),
    ("UMCSENT", "Consumer Confidence (UMich)", "core_macro"),
    ("FEDFUNDS", "Fed Funds Rate", "core_macro"),
    ("M2SL", "M2 Money Supply", "core_macro"),
    ("UNRATE", "Unemployment Rate", "core_macro"),
    ("TOTBKCR", "Total Bank Credit (All Commercial Banks)", "banking_system"),
    ("WALCL", "Fed Total Assets (Balance Sheet)", "banking_system"),
    ("DGS10", "10-Year Treasury Constant Maturity Yield", "bond_market"),
    ("DGS2", "2-Year Treasury Constant Maturity Yield", "bond_market"),
    ("BAMLC0A4CBBB", "ICE BofA BBB US Corporate OAS", "bond_market"),
    ("CPIAUCSL", "CPI All Items", "inflation"),
    ("PPIACO", "PPI All Commodities", "inflation"),
    ("GASREGW", "US Regular Gasoline Price", "supply_chain"),
    ("DCOILBRENTEU", "Brent Crude Oil (FRED)", "supply_chain"),
    ("INDPRO", "Industrial Production Index", "supply_chain"),
    # CPI component breakdown — where inflation is hitting hardest
    ("CUSR0000SAF11", "CPI Food at Home", "inflation_components"),
    ("CUSR0000SEFV", "CPI Food Away from Home", "inflation_components"),
    ("CUSR0000SAH1", "CPI Shelter", "inflation_components"),
    ("CUSR0000SEHA", "CPI Rent of Primary Residence", "inflation_components"),
    ("CPIENGSL", "CPI Energy", "inflation_components"),
    ("CUSR0000SAM", "CPI Medical Care", "inflation_components"),
    ("CUSR0000SETA02", "CPI Used Cars and Trucks", "inflation_components"),
    ("CPILFESL", "CPI Core (ex Food & Energy)", "inflation_components"),
    # Inflation expectations and alternative measures
    ("T10YIEM", "10Y Breakeven Inflation Rate", "inflation_expectations"),
    ("MICH", "UMich Inflation Expectations (1yr)", "inflation_expectations"),
    ("MEDCPIM158SFRBCLE", "Median CPI (Cleveland Fed)", "inflation_expectations"),
    ("PCETRIM12M159SFRBDAL", "Trimmed Mean PCE (Dallas Fed)", "inflation_expectations"),
]


@dataclass
class MacroIndicator:
    series_id: str
    name: str
    value: float
    category: str = "core_macro"  # core_macro | banking_system | bond_market
    observation_date: date | None = None  # last FRED observation date for this series
    previous_value: float | None = None
    change: float | None = None
    yoy_change: float | None = None  # year-over-year % change (first vs last in 365-day window)
    signal: str = "neutral"  # bullish, bearish, neutral, warning, critical
    description: str = ""


@dataclass
class MacroSnapshot:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    indicators: list[MacroIndicator] = field(default_factory=list)
    # Latest observation date among fetched series (each series has its own release cadence).
    fred_observations_through: date | None = None
    yield_curve_inverted: bool = False
    credit_stress: bool = False
    recession_signals: int = 0

    def to_prompt_text(self) -> str:
        """Format macro data for inclusion in Claude prompts."""
        lines = ["=== MACROECONOMIC & FINANCIAL STABILITY INDICATORS (FRED, OFFICIAL DATA) ==="]
        for ind in self.indicators:
            line = f"[{ind.category}] {ind.name}: {ind.value:.2f}"
            if ind.change is not None:
                line += f" (change: {ind.change:+.2f})"
            if ind.yoy_change is not None:
                line += f" (YoY: {ind.yoy_change:+.1f}%)"
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

    for series_id, name, category in FRED_SERIES:
        console.print(f"  Fetching {name} ({series_id})...", style="dim")
        indicator = _fetch_single_series(fred, series_id, name, category)
        if indicator:
            snapshot.indicators.append(indicator)

    _assess_yield_curve(snapshot)
    _assess_credit_conditions(snapshot)
    _count_recession_signals(snapshot)

    obs_dates = [ind.observation_date for ind in snapshot.indicators if ind.observation_date is not None]
    if obs_dates:
        snapshot.fred_observations_through = max(obs_dates)

    n = len(snapshot.indicators)
    if n == 0:
        console.print(
            "[yellow]FRED: 0 of "
            f"{len(FRED_SERIES)} series loaded — check API key, network, and pip install fredapi.[/yellow]"
        )
    else:
        console.print(f"  [dim]FRED: {n} macro indicators loaded.[/dim]")

    return snapshot


def _fetch_single_series(fred, series_id: str, name: str, category: str = "core_macro") -> MacroIndicator | None:
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

        year_ago = float(data.iloc[0]) if len(data) > 2 else None
        yoy_change: float | None = None
        if year_ago and year_ago > 0:
            yoy_change = ((current - year_ago) / year_ago) * 100

        last_idx = data.index[-1]
        if hasattr(last_idx, "date"):
            obs_date: date | None = last_idx.date()
        elif isinstance(last_idx, date):
            obs_date = last_idx
        else:
            obs_date = None

        indicator = MacroIndicator(
            series_id=series_id,
            name=name,
            value=current,
            category=category,
            observation_date=obs_date,
            previous_value=previous,
            change=change,
            yoy_change=yoy_change,
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

    elif sid == "TOTBKCR":
        # Weekly total bank credit (billions USD). System-wide lending stock, not one bank's health.
        if indicator.change is not None and indicator.change < -120:
            indicator.signal = "critical"
            indicator.description = "Very large contraction in total bank credit — strained credit conditions possible"
        elif indicator.change is not None and indicator.change < -60:
            indicator.signal = "warning"
            indicator.description = "Sharp weekly drop in bank credit — watch for tightening supply"
        elif indicator.change is not None and indicator.change > 80:
            indicator.signal = "neutral"
            indicator.description = "Bank credit expanding — typical growth environment signal"
        else:
            indicator.signal = "neutral"
            indicator.description = "Bank credit level stable vs prior week (aggregate; not institution CAMELS)"

    elif sid == "WALCL":
        # Fed balance sheet (millions USD). Direction indicates QT/QE, not bank solvency per se.
        if indicator.change is not None and indicator.change < -75000:
            indicator.signal = "neutral"
            indicator.description = "Balance sheet declining (asset run-off / QT-type dynamics)"
        elif indicator.change is not None and indicator.change > 75000:
            indicator.signal = "warning"
            indicator.description = "Large balance sheet increase — liquidity / asset purchases rising"
        else:
            indicator.signal = "neutral"
            indicator.description = "Modest weekly change in Fed assets (policy / runoff)"

    elif sid == "DGS10":
        if indicator.value >= 5.25:
            indicator.signal = "warning"
            indicator.description = "10Y yield very high — restrictive financing conditions for housing/Corporate"
        elif indicator.change is not None and indicator.change >= 0.12:
            indicator.signal = "bearish"
            indicator.description = "10Y yield jumping — duration assets under pressure"
        elif indicator.value <= 3.0 and indicator.change is not None and indicator.change <= -0.1:
            indicator.signal = "bullish"
            indicator.description = "10Y yield falling — easing financial conditions / flight to safety"
        else:
            indicator.signal = "neutral"
            indicator.description = "10Y in typical range for directional read (official Treasury/FRED)"

    elif sid == "DGS2":
        if indicator.value >= 5.0:
            indicator.signal = "warning"
            indicator.description = "Short-end yields very high — policy tight / inversion risk vs 10Y context"
        elif indicator.change is not None and indicator.change >= 0.15:
            indicator.signal = "bearish"
            indicator.description = "2Y rising fast — repricing Fed path / front-end pressure"
        else:
            indicator.signal = "neutral"
            indicator.description = "Policy-sensitive front-end yield (use with 10Y for curve shape)"

    elif sid == "BAMLC0A4CBBB":
        if indicator.value >= 2.75:
            indicator.signal = "critical"
            indicator.description = "BBB option-adjusted spread very wide — IG corporate stress"
        elif indicator.value >= 2.1:
            indicator.signal = "warning"
            indicator.description = "BBB spreads elevated vs norms — funding costs rising for lower IG"
        elif indicator.change is not None and indicator.change >= 0.15:
            indicator.signal = "bearish"
            indicator.description = "BBB OAS widening — credit risk repricing"
        else:
            indicator.signal = "neutral"
            indicator.description = "Investment-grade (BBB) spread within non-crisis band"

    elif sid == "UNRATE":
        if indicator.value >= 6.0:
            indicator.signal = "critical"
            indicator.description = "Unemployment at recession levels"
        elif indicator.value >= 5.0:
            indicator.signal = "warning"
            indicator.description = "Unemployment elevated — labor market deteriorating"
        elif indicator.change is not None and indicator.change >= 0.3:
            indicator.signal = "warning"
            indicator.description = f"Unemployment rising fast ({indicator.change:+.1f} pp) — Sahm Rule territory"
        elif indicator.change is not None and indicator.change >= 0.1:
            indicator.signal = "bearish"
            indicator.description = "Unemployment edging up"
        else:
            indicator.signal = "neutral"
            indicator.description = "Labor market healthy"

    elif sid == "CPIAUCSL":
        yoy = indicator.yoy_change
        if yoy is not None:
            if yoy > 6:
                indicator.signal = "critical"
                indicator.description = f"CPI surging ({yoy:+.1f}% YoY) — stagflation risk"
            elif yoy > 4:
                indicator.signal = "warning"
                indicator.description = f"CPI elevated ({yoy:+.1f}% YoY) — inflation sticky"
            elif yoy > 3:
                indicator.signal = "bearish"
                indicator.description = f"CPI above target ({yoy:+.1f}% YoY)"
            else:
                indicator.signal = "neutral"
                indicator.description = f"CPI near target ({yoy:+.1f}% YoY)"
        else:
            indicator.signal = "neutral"
            indicator.description = f"CPI level: {indicator.value:,.1f} (index)"

    elif sid == "PPIACO":
        if indicator.change is not None and indicator.change > 8:
            indicator.signal = "critical"
            indicator.description = "Producer prices surging — cost-push inflation accelerating"
        elif indicator.change is not None and indicator.change > 3:
            indicator.signal = "warning"
            indicator.description = "Producer prices rising — input costs increasing"
        elif indicator.change is not None and indicator.change < -3:
            indicator.signal = "bearish"
            indicator.description = "Producer prices falling — demand destruction signal"
        else:
            indicator.signal = "neutral"
            indicator.description = "Producer prices stable"

    elif sid == "GASREGW":
        if indicator.value >= 5.0:
            indicator.signal = "critical"
            indicator.description = f"Gasoline at ${indicator.value:.2f}/gal — consumer spending squeeze"
        elif indicator.value >= 4.0:
            indicator.signal = "warning"
            indicator.description = f"Gasoline at ${indicator.value:.2f}/gal — elevated energy costs"
        elif indicator.value >= 3.5:
            indicator.signal = "bearish"
            indicator.description = f"Gasoline at ${indicator.value:.2f}/gal — above comfort zone"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Gasoline at ${indicator.value:.2f}/gal — manageable"

    elif sid == "DCOILBRENTEU":
        if indicator.value >= 120:
            indicator.signal = "critical"
            indicator.description = f"Brent at ${indicator.value:.0f} — crisis-level oil prices"
        elif indicator.value >= 95:
            indicator.signal = "warning"
            indicator.description = f"Brent at ${indicator.value:.0f} — elevated, supply chain stress likely"
        elif indicator.value >= 80:
            indicator.signal = "bearish"
            indicator.description = f"Brent at ${indicator.value:.0f} — above long-term average"
        elif indicator.value <= 50:
            indicator.signal = "bearish"
            indicator.description = f"Brent at ${indicator.value:.0f} — demand destruction signal"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Brent at ${indicator.value:.0f} — typical range"

    elif sid == "INDPRO":
        if indicator.change is not None and indicator.change < -1.0:
            indicator.signal = "critical"
            indicator.description = "Industrial production contracting sharply — recession signal"
        elif indicator.change is not None and indicator.change < -0.3:
            indicator.signal = "warning"
            indicator.description = "Industrial production declining — manufacturing slowdown"
        elif indicator.change is not None and indicator.change > 0.5:
            indicator.signal = "bullish"
            indicator.description = "Industrial production growing — factory output expanding"
        else:
            indicator.signal = "neutral"
            indicator.description = "Industrial production stable"

    # --- CPI components (index values; signal based on YoY%) ---

    elif sid in ("CUSR0000SAF11", "CUSR0000SEFV", "CUSR0000SAH1", "CUSR0000SEHA",
                 "CPIENGSL", "CUSR0000SAM", "CUSR0000SETA02", "CPILFESL"):
        yoy = indicator.yoy_change
        if yoy is not None:
            if yoy > 6:
                indicator.signal = "critical"
                indicator.description = f"YoY {yoy:+.1f}% — running far above headline CPI"
            elif yoy > 4:
                indicator.signal = "warning"
                indicator.description = f"YoY {yoy:+.1f}% — outpacing headline inflation"
            elif yoy > 3:
                indicator.signal = "bearish"
                indicator.description = f"YoY {yoy:+.1f}% — above Fed 2% target"
            elif yoy < -1:
                indicator.signal = "bearish"
                indicator.description = f"YoY {yoy:+.1f}% — deflationary (demand weakness or correction)"
            else:
                indicator.signal = "neutral"
                indicator.description = f"YoY {yoy:+.1f}% — within normal range"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Index level: {indicator.value:,.1f} (insufficient data for YoY)"

    # --- Inflation expectations and alternative measures ---

    elif sid == "T10YIEM":
        if indicator.value > 3.5:
            indicator.signal = "critical"
            indicator.description = f"Breakeven at {indicator.value:.2f}% — market pricing persistent high inflation"
        elif indicator.value > 3.0:
            indicator.signal = "warning"
            indicator.description = f"Breakeven at {indicator.value:.2f}% — elevated inflation expectations"
        elif indicator.value < 1.5:
            indicator.signal = "bearish"
            indicator.description = f"Breakeven at {indicator.value:.2f}% — deflation/recession risk priced in"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Breakeven at {indicator.value:.2f}% — anchored near target"

    elif sid == "MICH":
        if indicator.value > 5.0:
            indicator.signal = "critical"
            indicator.description = f"Consumer expects {indicator.value:.1f}% inflation — expectations unanchored"
        elif indicator.value > 4.0:
            indicator.signal = "warning"
            indicator.description = f"Consumer expects {indicator.value:.1f}% inflation — elevated, self-fulfilling risk"
        elif indicator.value > 3.0:
            indicator.signal = "bearish"
            indicator.description = f"Consumer expects {indicator.value:.1f}% inflation — above Fed comfort zone"
        else:
            indicator.signal = "neutral"
            indicator.description = f"Consumer expects {indicator.value:.1f}% inflation — well-anchored"

    elif sid in ("MEDCPIM158SFRBCLE", "PCETRIM12M159SFRBDAL"):
        label = "Median CPI" if sid == "MEDCPIM158SFRBCLE" else "Trimmed Mean PCE"
        if indicator.value > 5.0:
            indicator.signal = "critical"
            indicator.description = f"{label} at {indicator.value:.1f}% — broad-based inflation entrenched"
        elif indicator.value > 4.0:
            indicator.signal = "warning"
            indicator.description = f"{label} at {indicator.value:.1f}% — inflation widespread, not outlier-driven"
        elif indicator.value > 3.0:
            indicator.signal = "bearish"
            indicator.description = f"{label} at {indicator.value:.1f}% — above target but narrowing"
        else:
            indicator.signal = "neutral"
            indicator.description = f"{label} at {indicator.value:.1f}% — near or below 2% target"


def apply_derived_macro_flags(snapshot: MacroSnapshot) -> None:
    """Recompute flags from `snapshot.indicators` (yield inversion, credit stress, recession tally)."""
    _assess_yield_curve(snapshot)
    _assess_credit_conditions(snapshot)
    _count_recession_signals(snapshot)


def classify_macro_observation(indicator: MacroIndicator) -> None:
    """Apply the same rules as live FRED fetch (for historical replay rows). Mutates `indicator`."""
    _classify_signal(indicator)


def _assess_yield_curve(snapshot: MacroSnapshot):
    for ind in snapshot.indicators:
        if ind.series_id in ("T10Y2Y", "T10Y3M") and ind.value < 0:
            snapshot.yield_curve_inverted = True
            break


def _assess_credit_conditions(snapshot: MacroSnapshot):
    snapshot.credit_stress = any(
        (ind.series_id == "BAMLH0A0HYM2" and ind.value > 4.5)
        or (ind.series_id == "BAMLC0A4CBBB" and ind.value > 2.35)
        for ind in snapshot.indicators
    )


def _count_recession_signals(snapshot: MacroSnapshot):
    count = 0
    for ind in snapshot.indicators:
        if ind.signal in ("critical", "warning"):
            count += 1
    snapshot.recession_signals = count
