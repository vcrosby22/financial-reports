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


def _bond_item(tag_class: str, tag_label: str, title: str, skim: str, body_html: str) -> str:
    """One scannable row: summary always visible; full copy inside nested <details>."""
    return (
        '<details class="bond-bank-item">'
        '<summary class="bond-bank-summary">'
        f'<span class="tag {tag_class}">{tag_label}</span> '
        f"<strong>{title}</strong> — {skim}"
        "</summary>"
        f'<div class="bond-bank-item-body">{body_html}</div>'
        "</details>"
    )


def build_bond_bank_friend_html(macro_data: MacroSnapshot | None) -> str:
    """Return HTML fragment for the report (safe literals; numeric values formatted)."""
    if macro_data is None or not macro_data.indicators:
        return (
            '<p style="color:var(--text-dim);">Macro detail from FRED was not available for this run '
            "(add <code>FRED_API_KEY</code> to fill in banking and bond indicators). "
            "Market and risk sections above may still be useful.</p>"
        )

    intro = """<div class="bond-bank-intro">
<ul class="bond-bank-intro-list">
<li><strong>Whole-system snapshot</strong> — big-picture context from mostly official data, same series as the macro tables.</li>
<li><strong>Not about one bank</strong> — not a rating of your institution; aggregate series only.</li>
<li><strong>Not CAMELS / not exams</strong> — not a substitute for supervisory information you can&rsquo;t look up like a stock quote.</li>
</ul>
</div>
<div class="bond-bank-scan">
"""

    items: list[str] = []

    if macro_data.yield_curve_inverted:
        items.append(
            _bond_item(
                "tag-warning",
                "Watch",
                "Yield curve",
                "Short-term Treasuries above long-term in our measures — a classic pattern, not a timer.",
                "<p><strong>Yield curve.</strong> In the spreads we track, shorter-term Treasury yields are "
                "above longer-term ones — a pattern that has often appeared before recessions in the past, "
                "but it is <em>not</em> a timer and <em>not</em> a guarantee.</p>",
            )
        )
    else:
        items.append(
            _bond_item(
                "tag-info",
                "OK",
                "Yield curve",
                "Not inverted in the measures we track — one warning light off; other risks still possible.",
                "<p><strong>Yield curve.</strong> In the measures we track, the curve is not inverted right now — "
                "one classic warning light is off, though other risks can still show up elsewhere.</p>",
            )
        )

    hy = _get_ind(macro_data, "BAMLH0A0HYM2")
    bbb = _get_ind(macro_data, "BAMLC0A4CBBB")
    if hy or bbb:
        if macro_data.credit_stress:
            items.append(
                _bond_item(
                    "tag-warning",
                    "Stressed",
                    "Corporate borrowing costs",
                    "HY/BBB spreads look strained versus our cutoffs — context for funding stress, not a default forecast.",
                    "<p><strong>Corporate borrowing costs.</strong> In the high-yield and BBB spread series we track, "
                    "levels look <strong>stressed versus our rule-of-thumb cutoffs</strong>. "
                    "That usually means markets want extra yield to lend to riskier or lower-rated firms — "
                    "useful context, not a prediction of defaults.</p>",
                )
            )
        else:
            items.append(
                _bond_item(
                    "tag-info",
                    "Calm",
                    "Corporate borrowing costs",
                    "Spreads are not flashing our credit-stress rule — calmer corporate funding than a stressed episode.",
                    "<p><strong>Corporate borrowing costs.</strong> Those spreads are <strong>not</strong> flashing "
                    "our &ldquo;credit stress&rdquo; rule right now — think calmer corporate funding conditions "
                    "than in a stressed credit episode (still watch the detailed table above).</p>",
                )
            )

    tot = _get_ind(macro_data, "TOTBKCR")
    wal = _get_ind(macro_data, "WALCL")

    if tot:
        ch = tot.change
        if ch is not None and ch < -60:
            items.append(
                _bond_item(
                    "tag-warning",
                    "Tight",
                    "Bank lending (aggregate)",
                    "Total bank credit fell meaningfully week over week — possible tighter credit; still system-wide, not your bank.",
                    "<p><strong>Bank lending (aggregate).</strong> Total bank credit fell meaningfully in the "
                    "latest week-over-week reading. That can mean tighter credit availability across the economy — "
                    "still an <em>aggregate</em> loan book, not a verdict on your bank.</p>",
                )
            )
        elif ch is not None and ch > 80:
            items.append(
                _bond_item(
                    "tag-info",
                    "Growing",
                    "Bank lending (aggregate)",
                    "Credit stock up in the latest reading — more economy-wide borrowing at a high level.",
                    "<p><strong>Bank lending (aggregate).</strong> Total bank credit grew in the latest reading — "
                    "consistent with more borrowing in the economy at a high level.</p>",
                )
            )
        else:
            items.append(
                _bond_item(
                    "tag-info",
                    "Neutral",
                    "Bank lending (aggregate)",
                    "No extreme week-over-week move by our thresholds — whole-system credit stock, not one bank.",
                    "<p><strong>Bank lending (aggregate).</strong> The latest week did not show an extreme move by our "
                    "thresholds. This series is the whole banking system&rsquo;s credit stock, not one institution.</p>",
                )
            )

    if wal:
        ch = wal.change
        if ch is not None and ch > 75_000:
            items.append(
                _bond_item(
                    "tag-warning",
                    "Expand",
                    "Federal Reserve balance sheet",
                    "Fed assets up a lot — often liquidity or balance-sheet expansion; policy backdrop, not deposit insurance.",
                    "<p><strong>Federal Reserve balance sheet.</strong> Fed total assets rose a lot in the latest "
                    "period — often associated with liquidity support or balance-sheet expansion. "
                    "This is about <em>policy and system liquidity</em>, not a deposit-insurance scorecard.</p>",
                )
            )
        elif ch is not None and ch < -75_000:
            items.append(
                _bond_item(
                    "tag-info",
                    "Shrink",
                    "Federal Reserve balance sheet",
                    "Fed assets down — often run-off or balance-sheet shrinkage.",
                    "<p><strong>Federal Reserve balance sheet.</strong> Fed assets declined — often consistent with "
                    "run-off or balance-sheet shrinkage.</p>",
                )
            )
        else:
            items.append(
                _bond_item(
                    "tag-info",
                    "Stable",
                    "Federal Reserve balance sheet",
                    "Moderate weekly change — backdrop for liquidity and asset holdings.",
                    "<p><strong>Federal Reserve balance sheet.</strong> The latest weekly change was moderate — "
                    "a backdrop indicator for liquidity and asset holdings.</p>",
                )
            )

    d2 = _get_ind(macro_data, "DGS2")
    d10 = _get_ind(macro_data, "DGS10")
    if d2 and d10:
        items.append(
            _bond_item(
                "tag-info",
                "Levels",
                "Treasury yields",
                "2y and 10y levels anchor expectations and long borrowing; compare curve rows in macro for shape.",
                "<p><strong>Treasury yields.</strong> The 2-year (sensitive to Fed expectations) is about "
                f"<strong>{d2.value:.2f}%</strong>; the 10-year (longer financing and mortgage anchor) about "
                f"<strong>{d10.value:.2f}%</strong>. "
                "When short rates sit well above long rates, markets are often weighing slower growth or future "
                "cuts — compare with the curve rows in the macro table.</p>",
            )
        )
    else:
        t10y2y = _get_ind(macro_data, "T10Y2Y")
        if t10y2y:
            items.append(
                _bond_item(
                    "tag-info",
                    "Spread",
                    "Yield curve shape",
                    "10y minus 2y spread in one number — see macro for other curve measures.",
                    "<p><strong>Yield curve shape.</strong> The 10-year minus 2-year spread is about "
                    f"<strong>{t10y2y.value:.2f}</strong> (percentage points). "
                    "See the macro section for other curve measures.</p>",
                )
            )

    items.append(
        _bond_item(
            "tag-moderate",
            "Note",
            "Reality check",
            "Rules on delayed public data — orientation for people you care about, not trades or deposit advice.",
            "<p style=\"font-size:0.85rem;color:var(--text-dim);\"><strong>Reality check.</strong> "
            "These sentences are simple rules on top of delayed public data — orientation for people you care about, "
            "not trading instructions or deposit advice.</p>",
        )
    )

    return intro + "\n".join(items) + "\n</div>"
