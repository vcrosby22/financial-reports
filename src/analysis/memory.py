"""Analysis memory — builds trend context from historical snapshots.

Solves the "no memory across analyses" weakness by querying the database
for past snapshots and constructing a "what changed" summary that gets
injected into Claude's prompt.
"""

from datetime import datetime, timedelta

from sqlalchemy import desc

from ..data.database import get_session, init_db
from ..data.models import AnalysisReport, MarketSnapshot


def build_trend_context(lookback_days: int = 7) -> str:
    """Build a text summary of what changed since the last analysis.

    Queries the database for historical snapshots and compares
    to identify trends, new signals, and deteriorating conditions.
    """
    init_db()
    session = get_session()

    try:
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        sections = []

        last_report = session.query(AnalysisReport).order_by(
            desc(AnalysisReport.timestamp)
        ).first()

        if last_report:
            sections.append(
                f"Last AI analysis: {last_report.timestamp.strftime('%Y-%m-%d %H:%M')} "
                f"— Risk level was: {(last_report.risk_level or 'unknown').upper()}"
            )

        key_tickers = ["^GSPC", "^VIX", "^TNX", "^DJI"]
        ticker_trends = []

        for ticker in key_tickers:
            snapshots = (
                session.query(MarketSnapshot)
                .filter(MarketSnapshot.ticker == ticker, MarketSnapshot.timestamp >= cutoff)
                .order_by(MarketSnapshot.timestamp)
                .all()
            )

            if len(snapshots) < 2:
                continue

            first = snapshots[0]
            latest = snapshots[-1]

            if first.price and latest.price and first.price != 0:
                change_pct = ((latest.price - first.price) / first.price) * 100
                ticker_trends.append(
                    f"{ticker}: {first.price:.2f} → {latest.price:.2f} "
                    f"({change_pct:+.2f}% over {len(snapshots)} snapshots)"
                )

        if ticker_trends:
            sections.append("\nTrend over last {} days:".format(lookback_days))
            sections.extend(ticker_trends)

        snapshot_count = (
            session.query(MarketSnapshot)
            .filter(MarketSnapshot.timestamp >= cutoff)
            .count()
        )
        sections.append(f"\nData points in last {lookback_days} days: {snapshot_count}")

        if not sections:
            return "No historical data available — this is the first analysis."

        return "\n".join(sections)

    finally:
        session.close()
