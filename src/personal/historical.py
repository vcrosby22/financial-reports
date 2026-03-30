"""Historical crash recovery database — structured data for every major US market collapse.

Used by both the personal defense dashboard and the public report's
Historical Parallels section. All data from publicly available sources.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class CrashEvent:
    name: str
    trigger: str
    peak_date: date
    trough_date: date
    peak_level: float          # S&P 500 (or DJIA-equivalent for pre-S&P era)
    trough_level: float
    decline_pct: float         # negative number (e.g. -56.8)
    days_to_bottom: int
    recovery_date: date | None # date the index recovered to prior peak (None if still ongoing)
    days_to_recovery: int | None  # from peak to full recovery
    months_to_recovery: float | None
    index_used: str            # "DJIA" for pre-1957, "S&P 500" after
    oil_shock: bool
    withdrawal_correct: bool   # Would 401(k) withdrawal at age 54 have beaten staying invested?
    withdrawal_notes: str
    parallels_to_2026: str     # What makes this relevant to the current situation


CRASHES: list[CrashEvent] = [
    CrashEvent(
        name="Panic of 1907",
        trigger="Trust company failures, copper speculation collapse, no central bank",
        peak_date=date(1906, 9, 1),
        trough_date=date(1907, 11, 15),
        peak_level=103.0,
        trough_level=53.0,
        decline_pct=-48.5,
        days_to_bottom=440,
        recovery_date=date(1909, 6, 1),
        days_to_recovery=1004,
        months_to_recovery=33,
        index_used="DJIA",
        oil_shock=False,
        withdrawal_correct=False,
        withdrawal_notes="Recovery in ~2.75 years. No 401(k) existed, but equivalent investor recovered fully.",
        parallels_to_2026="Banking system panic; no central bank backstop (Fed created in 1913 as a direct result).",
    ),
    CrashEvent(
        name="1929 Great Depression",
        trigger="Speculative bubble, margin debt, bank failures, tariff wars",
        peak_date=date(1929, 9, 7),
        trough_date=date(1932, 6, 1),
        peak_level=31.86,
        trough_level=4.40,
        decline_pct=-86.2,
        days_to_bottom=998,
        recovery_date=date(1954, 11, 23),
        days_to_recovery=9209,
        months_to_recovery=302,
        index_used="DJIA",
        oil_shock=False,
        withdrawal_correct=True,
        withdrawal_notes="THE ONLY CRASH where withdrawal would have been correct. 25-year recovery. "
                         "But context: no FDIC, no SEC, no unemployment insurance, banks physically closed. "
                         "Modern safeguards (FDIC, Fed, SEC, circuit breakers) make a repeat nearly impossible.",
        parallels_to_2026="Tariff wars (Smoot-Hawley then, trade tensions now). But modern safeguards are incomparably stronger.",
    ),
    CrashEvent(
        name="1937-1938 Recession",
        trigger="Premature Fed tightening, fiscal austerity after New Deal spending",
        peak_date=date(1937, 3, 6),
        trough_date=date(1938, 3, 31),
        peak_level=18.68,
        trough_level=8.50,
        decline_pct=-54.5,
        days_to_bottom=390,
        recovery_date=date(1945, 1, 1),
        days_to_recovery=2858,
        months_to_recovery=94,
        index_used="S&P 500",
        oil_shock=False,
        withdrawal_correct=False,
        withdrawal_notes="Recovery delayed by WWII, but investor at 54 would have recovered by 62. "
                         "Wartime economy eventually drove massive expansion.",
        parallels_to_2026="Fed policy mistake (tightening too early/holding too long). Directly relevant to 2026 Fed dilemma.",
    ),
    CrashEvent(
        name="1973-1974 Oil Crisis",
        trigger="OPEC oil embargo, Yom Kippur War, Nixon resignation, stagflation",
        peak_date=date(1973, 1, 11),
        trough_date=date(1974, 10, 3),
        peak_level=120.24,
        trough_level=62.28,
        decline_pct=-48.2,
        days_to_bottom=630,
        recovery_date=date(1980, 7, 17),
        days_to_recovery=2744,
        months_to_recovery=90,
        index_used="S&P 500",
        oil_shock=True,
        withdrawal_correct=False,
        withdrawal_notes="8-year recovery. Long, but investor at 54 recovered by 62. "
                         "Withdrawal at 48% tax+penalty would have been worse than riding it out. "
                         "CRITICAL: Stagflation era — inflation eroded both invested AND withdrawn money.",
        parallels_to_2026="CLOSEST PARALLEL. Oil shock + war + stagflation. Same Fed trap (can't cut into inflation, "
                          "can't hold as economy weakens). But 2026 supply chain damage is broader (helium, LNG, fertilizer).",
    ),
    CrashEvent(
        name="1987 Black Monday",
        trigger="Program trading, portfolio insurance, overvaluation, rising interest rates",
        peak_date=date(1987, 8, 25),
        trough_date=date(1987, 12, 4),
        peak_level=336.77,
        trough_level=223.92,
        decline_pct=-33.5,
        days_to_bottom=101,
        recovery_date=date(1989, 7, 26),
        days_to_recovery=700,
        months_to_recovery=23,
        index_used="S&P 500",
        oil_shock=False,
        withdrawal_correct=False,
        withdrawal_notes="23-month recovery. Fast. Withdrawal would have cost 39% "
                         "while the market lost 33% and recovered in under 2 years.",
        parallels_to_2026="Speed of decline (single-day crash). Circuit breakers now prevent exact repeat. "
                          "Less relevant — no underlying economic crisis.",
    ),
    CrashEvent(
        name="2000-2002 Dot-Com Bust",
        trigger="Tech bubble burst, overvaluation (Shiller CAPE ~44), 9/11, accounting scandals",
        peak_date=date(2000, 3, 24),
        trough_date=date(2002, 10, 9),
        peak_level=1527.46,
        trough_level=776.76,
        decline_pct=-49.1,
        days_to_bottom=929,
        recovery_date=date(2007, 5, 30),
        days_to_recovery=2623,
        months_to_recovery=86,
        index_used="S&P 500",
        oil_shock=False,
        withdrawal_correct=False,
        withdrawal_notes="7-year recovery to 2000 peak, then immediately crashed again in 2008. "
                         "True recovery from 2000 levels: ~13 years (March 2013). "
                         "But even with the double crash, staying invested beat a 39% withdrawal penalty. "
                         "Investor at 54 in 2000 would have recovered by 67 — past 65 but still recovered.",
        parallels_to_2026="High CAPE ratio (44 then, 40.7 now). Overvaluation as an amplifier. "
                          "But 2000 was tech-specific; 2026 is a broad supply shock.",
    ),
    CrashEvent(
        name="2007-2009 Global Financial Crisis",
        trigger="Subprime mortgage collapse, Lehman Brothers bankruptcy, credit freeze, bank failures",
        peak_date=date(2007, 10, 9),
        trough_date=date(2009, 3, 9),
        peak_level=1565.15,
        trough_level=676.53,
        decline_pct=-56.8,
        days_to_bottom=517,
        recovery_date=date(2013, 3, 28),
        days_to_recovery=2000,
        months_to_recovery=66,
        index_used="S&P 500",
        oil_shock=False,
        withdrawal_correct=False,
        withdrawal_notes="5.5-year recovery from peak. The worst modern crash. "
                         "Even with a 57% market drop, the 39% withdrawal penalty was STILL worse because "
                         "the market recovered. Investor at 54 in 2007 was whole by 60.",
        parallels_to_2026="Financial system stress, VIX spikes, credit spreads widening. "
                          "But 2008 was a financial/credit crisis; 2026 is a supply/commodity crisis.",
    ),
    CrashEvent(
        name="2020 COVID-19 Crash",
        trigger="Global pandemic, lockdowns, economic shutdown",
        peak_date=date(2020, 2, 19),
        trough_date=date(2020, 3, 23),
        peak_level=3386.15,
        trough_level=2237.40,
        decline_pct=-33.9,
        days_to_bottom=33,
        recovery_date=date(2020, 8, 18),
        days_to_recovery=181,
        months_to_recovery=6,
        index_used="S&P 500",
        oil_shock=True,
        withdrawal_correct=False,
        withdrawal_notes="6-month recovery. Anyone who withdrew paid 39% for nothing. "
                         "Oil also crashed (briefly negative). Fastest recovery in market history.",
        parallels_to_2026="Sudden external shock, oil price disruption. But COVID recovery was driven "
                          "by unprecedented fiscal stimulus (~$5T). Similar fiscal response unlikely in 2026 "
                          "given current debt levels and inflation.",
    ),
]

_2026_PEAK_DATE = date(2026, 1, 15)
_2026_PEAK_LEVEL = 6900.0


def build_current_crisis_event(sp500_price: float | None = None) -> CrashEvent:
    """Build a live 2026 crisis entry from the current S&P 500 price."""
    today = date.today()
    price = sp500_price if sp500_price else _2026_PEAK_LEVEL
    trough = min(price, _2026_PEAK_LEVEL)
    decline = ((trough - _2026_PEAK_LEVEL) / _2026_PEAK_LEVEL) * 100 if _2026_PEAK_LEVEL else 0
    days = (today - _2026_PEAK_DATE).days

    return CrashEvent(
        name="2026 Iran War / Strait of Hormuz (ONGOING)",
        trigger="Iran war, Strait of Hormuz closure, oil shock, Ras Laffan destruction, supply chain cascade",
        peak_date=_2026_PEAK_DATE,
        trough_date=today,
        peak_level=_2026_PEAK_LEVEL,
        trough_level=trough,
        decline_pct=round(decline, 1),
        days_to_bottom=days,
        recovery_date=None,
        days_to_recovery=None,
        months_to_recovery=None,
        index_used="S&P 500",
        oil_shock=True,
        withdrawal_correct=False,
        withdrawal_notes=f"ONGOING. At {decline:.1f}%, far from the -39% breakeven for withdrawal. "
                         "Key difference from past oil shocks: physical infrastructure destruction "
                         "means 3-5 year supply chain disruption even after ceasefire.",
        parallels_to_2026="This IS 2026. Closest historical parallel: 1973 oil crisis + stagflation.",
    )


def get_all_crashes(sp500_price: float | None = None) -> list[CrashEvent]:
    """Return all historical crashes plus the live 2026 entry."""
    return CRASHES + [build_current_crisis_event(sp500_price)]


def find_similar_crashes(current_decline_pct: float, is_oil_shock: bool = True, sp500_price: float | None = None) -> list[CrashEvent]:
    """Find historical crashes with similar characteristics to the current situation."""
    scored: list[tuple[float, CrashEvent]] = []
    for crash in get_all_crashes(sp500_price):
        if crash.name.startswith("2026"):
            continue
        score = 0.0
        decline_diff = abs(abs(current_decline_pct) - abs(crash.decline_pct))
        if decline_diff < 10:
            score += 3.0
        elif decline_diff < 20:
            score += 1.5
        if is_oil_shock and crash.oil_shock:
            score += 4.0
        if crash.parallels_to_2026 and "CLOSEST" in crash.parallels_to_2026:
            score += 2.0
        scored.append((score, crash))
    scored.sort(key=lambda x: -x[0])
    return [crash for _, crash in scored if _ > 0]


def withdrawal_verdict_summary(sp500_price: float | None = None) -> str:
    """Summarize across all crashes: how often was withdrawal the right call?"""
    all_crashes = get_all_crashes(sp500_price)
    total = len([c for c in all_crashes if not c.name.startswith("2026")])
    correct = sum(1 for c in all_crashes if c.withdrawal_correct and not c.name.startswith("2026"))
    return (
        f"Across {total} major US market crashes (1907-2020), early withdrawal "
        f"would have been the mathematically correct choice in {correct} out of {total} cases "
        f"({'only 1929 — and only because the Great Depression took 25 years to recover, '
          'under conditions that cannot recur (no FDIC, no SEC, no Fed backstop)' if correct == 1 else 'none'}). "
        f"In every other crash, the market recovered faster than the penalty cost."
    )


def crash_comparison_for_dashboard(
    sp500_current: float,
    sp500_peak: float = 6900,
) -> dict:
    """Generate a comparison summary for the personal dashboard."""
    current_decline = ((sp500_current - sp500_peak) / sp500_peak) * 100

    similar = find_similar_crashes(current_decline, is_oil_shock=True, sp500_price=sp500_current)
    best_match = similar[0] if similar else None

    all_crashes = get_all_crashes(sp500_current)
    past_oil = [c for c in all_crashes if c.oil_shock and not c.name.startswith("2026")]
    avg_oil_decline = sum(abs(c.decline_pct) for c in past_oil) / len(past_oil) if past_oil else 0
    avg_oil_recovery_months = sum(
        c.months_to_recovery for c in past_oil if c.months_to_recovery
    ) / len([c for c in past_oil if c.months_to_recovery]) if past_oil else 0

    worst_non_depression = max(
        (c for c in all_crashes if not c.name.startswith("2026") and c.name != "1929 Great Depression"),
        key=lambda c: abs(c.decline_pct),
    )

    return {
        "current_decline_pct": current_decline,
        "best_match": best_match,
        "similar_crashes": similar[:3],
        "avg_oil_crash_decline": avg_oil_decline,
        "avg_oil_crash_recovery_months": avg_oil_recovery_months,
        "worst_non_depression": worst_non_depression,
        "withdrawal_verdict": withdrawal_verdict_summary(sp500_current),
        "crashes_where_withdrawal_correct": [c for c in all_crashes if c.withdrawal_correct],
    }
