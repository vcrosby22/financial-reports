"""Plain-language banking & bond context for non-expert readers (friends & family).

Uses the same public data as the macro table — system-wide aggregates and market prices —
without implying CAMELS, exam ratings, or any single institution's health.
"""

from __future__ import annotations

from ..data.macro import MacroSnapshot


def _get_ind(snapshot: MacroSnapshot, series_id: str):
    for ind in snapshot.indicators:
        if ind.series_id == series_id:
            return ind
    return None


def build_bond_bank_friend_html(macro_data: MacroSnapshot | None) -> str:
    """Return HTML fragment for the report (safe literals; numeric values formatted)."""
    if macro_data is None or not macro_data.indicators:
        return (
            '<p style="color:var(--text-dim);">Macro detail from FRED was not available for this run '
            "(add <code>FRED_API_KEY</code> to fill in banking and bond indicators). "
            "Market and risk sections above may still be useful.</p>"
        )

    parts: list[str] = []

    parts.append(
        "<p><strong>Who this is for.</strong> This is a <em>big-picture</em>, <em>whole-system</em> snapshot "
        "from mostly official data. It is <strong>not</strong> a rating of any one bank, and it is "
        "<strong>not</strong> a substitute for supervisory information (regulators review banks in depth; "
        "those exam-style grades are not something you can look up like a stock quote).</p>"
    )

    if macro_data.yield_curve_inverted:
        parts.append(
            "<p><strong>Yield curve.</strong> In the spreads we track, shorter-term Treasury yields are "
            "above longer-term ones — a pattern that has often appeared before recessions in the past, "
            "but it is <em>not</em> a timer and <em>not</em> a guarantee.</p>"
        )
    else:
        parts.append(
            "<p><strong>Yield curve.</strong> In the measures we track, the curve is not inverted right now — "
            "one classic warning light is off, though other risks can still show up elsewhere.</p>"
        )

    hy = _get_ind(macro_data, "BAMLH0A0HYM2")
    bbb = _get_ind(macro_data, "BAMLC0A4CBBB")
    if hy or bbb:
        if macro_data.credit_stress:
            parts.append(
                "<p><strong>Corporate borrowing costs.</strong> In the high-yield and BBB spread series we track, "
                "levels look <strong>stressed versus our rule-of-thumb cutoffs</strong>. "
                "That usually means markets want extra yield to lend to riskier or lower-rated firms — "
                "useful context, not a prediction of defaults.</p>"
            )
        else:
            parts.append(
                "<p><strong>Corporate borrowing costs.</strong> Those spreads are <strong>not</strong> flashing "
                "our &ldquo;credit stress&rdquo; rule right now — think calmer corporate funding conditions "
                "than in a stressed credit episode (still watch the detailed table above).</p>"
            )

    tot = _get_ind(macro_data, "TOTBKCR")
    wal = _get_ind(macro_data, "WALCL")

    if tot:
        ch = tot.change
        if ch is not None and ch < -60:
            parts.append(
                "<p><strong>Bank lending (aggregate).</strong> Total bank credit fell meaningfully in the "
                "latest week-over-week reading. That can mean tighter credit availability across the economy — "
                "still an <em>aggregate</em> loan book, not a verdict on your bank.</p>"
            )
        elif ch is not None and ch > 80:
            parts.append(
                "<p><strong>Bank lending (aggregate).</strong> Total bank credit grew in the latest reading — "
                "consistent with more borrowing in the economy at a high level.</p>"
            )
        else:
            parts.append(
                "<p><strong>Bank lending (aggregate).</strong> The latest week did not show an extreme move by our "
                "thresholds. This series is the whole banking system&rsquo;s credit stock, not one institution.</p>"
            )

    if wal:
        ch = wal.change
        if ch is not None and ch > 75_000:
            parts.append(
                "<p><strong>Federal Reserve balance sheet.</strong> Fed total assets rose a lot in the latest "
                "period — often associated with liquidity support or balance-sheet expansion. "
                "This is about <em>policy and system liquidity</em>, not a deposit-insurance scorecard.</p>"
            )
        elif ch is not None and ch < -75_000:
            parts.append(
                "<p><strong>Federal Reserve balance sheet.</strong> Fed assets declined — often consistent with "
                "run-off or balance-sheet shrinkage.</p>"
            )
        else:
            parts.append(
                "<p><strong>Federal Reserve balance sheet.</strong> The latest weekly change was moderate — "
                "a backdrop indicator for liquidity and asset holdings.</p>"
            )

    d2 = _get_ind(macro_data, "DGS2")
    d10 = _get_ind(macro_data, "DGS10")
    if d2 and d10:
        parts.append(
            "<p><strong>Treasury yields.</strong> The 2-year (sensitive to Fed expectations) is about "
            f"<strong>{d2.value:.2f}%</strong>; the 10-year (longer financing and mortgage anchor) about "
            f"<strong>{d10.value:.2f}%</strong>. "
            "When short rates sit well above long rates, markets are often weighing slower growth or future "
            "cuts — compare with the curve rows in the macro table.</p>"
        )
    else:
        t10y2y = _get_ind(macro_data, "T10Y2Y")
        if t10y2y:
            parts.append(
                "<p><strong>Yield curve shape.</strong> The 10-year minus 2-year spread is about "
                f"<strong>{t10y2y.value:.2f}</strong> (percentage points). "
                "See the macro section for other curve measures.</p>"
            )

    parts.append(
        "<p style=\"font-size:0.85rem;color:var(--text-dim);\"><strong>Reality check.</strong> "
        "These sentences are simple rules on top of delayed public data — orientation for people you care about, "
        "not trading instructions or deposit advice.</p>"
    )

    return "\n".join(parts)
