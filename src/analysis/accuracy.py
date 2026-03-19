"""Prediction accuracy tracker — logs predictions and checks outcomes.

Creates a feedback loop: the system surfaces opportunities, logs them,
and later checks whether the risk scores matched reality. Over time,
this reveals which signal combinations are actually predictive.
"""

from datetime import datetime, timedelta

import yfinance as yf
from rich.console import Console
from rich.table import Table
from sqlalchemy import and_

from ..data.database import get_session, init_db
from ..data.models import Prediction

console = Console()


def log_prediction(
    ticker: str,
    direction: str,
    risk_score: int,
    confidence: str,
    price: float,
    time_horizon: str = "",
    reasoning: str = "",
):
    """Log a new prediction for future accuracy tracking."""
    init_db()
    session = get_session()
    try:
        prediction = Prediction(
            ticker=ticker,
            direction=direction,
            risk_score=risk_score,
            confidence=confidence,
            price_at_prediction=price,
            time_horizon=time_horizon,
            reasoning=reasoning,
            outcome="pending",
        )
        session.add(prediction)
        session.commit()
    finally:
        session.close()


def check_predictions():
    """Check all pending predictions against current prices.

    Updates 7d, 30d, 60d, 90d price and return columns.
    """
    init_db()
    session = get_session()

    try:
        pending = session.query(Prediction).filter(
            Prediction.outcome == "pending"
        ).all()

        if not pending:
            console.print("[dim]No pending predictions to check.[/dim]")
            return

        console.print(f"[bold]Checking {len(pending)} pending predictions...[/bold]")

        for pred in pending:
            age_days = (datetime.utcnow() - pred.timestamp).days

            try:
                ticker = yf.Ticker(pred.ticker)
                current_price = ticker.history(period="1d")["Close"].iloc[-1]
            except Exception:
                continue

            if age_days >= 7 and pred.price_7d is None:
                pred.price_7d = current_price
                pred.return_7d = _calc_return(pred.price_at_prediction, current_price, pred.direction)

            if age_days >= 30 and pred.price_30d is None:
                pred.price_30d = current_price
                pred.return_30d = _calc_return(pred.price_at_prediction, current_price, pred.direction)

            if age_days >= 60 and pred.price_60d is None:
                pred.price_60d = current_price
                pred.return_60d = _calc_return(pred.price_at_prediction, current_price, pred.direction)

            if age_days >= 90 and pred.price_90d is None:
                pred.price_90d = current_price
                pred.return_90d = _calc_return(pred.price_at_prediction, current_price, pred.direction)
                _assess_outcome(pred)

        session.commit()
    finally:
        session.close()


def get_accuracy_report() -> str:
    """Generate accuracy stats for Claude's context calibration."""
    init_db()
    session = get_session()

    try:
        all_preds = session.query(Prediction).all()
        if not all_preds:
            return "No prediction history yet."

        completed = [p for p in all_preds if p.outcome and p.outcome != "pending"]
        pending_count = sum(1 for p in all_preds if p.outcome == "pending")

        if not completed:
            return f"Predictions logged: {len(all_preds)} ({pending_count} pending, none completed yet)"

        correct = sum(1 for p in completed if p.outcome == "correct")
        accuracy = correct / len(completed) * 100 if completed else 0

        low_risk = [p for p in completed if p.risk_score <= 3]
        low_risk_correct = sum(1 for p in low_risk if p.outcome == "correct")
        low_risk_accuracy = low_risk_correct / len(low_risk) * 100 if low_risk else 0

        return (
            f"Prediction accuracy: {accuracy:.0f}% ({correct}/{len(completed)} correct)\n"
            f"Low-risk accuracy: {low_risk_accuracy:.0f}% ({low_risk_correct}/{len(low_risk)})\n"
            f"Pending: {pending_count} | Total logged: {len(all_preds)}"
        )
    finally:
        session.close()


def display_predictions():
    """Display all predictions with their current status."""
    init_db()
    session = get_session()

    try:
        predictions = session.query(Prediction).order_by(Prediction.timestamp.desc()).limit(20).all()

        if not predictions:
            console.print("[dim]No predictions logged yet.[/dim]")
            return

        table = Table(title="Prediction Tracker", show_header=True, header_style="bold cyan")
        table.add_column("Date", width=10)
        table.add_column("Ticker", style="bold")
        table.add_column("Dir", width=5)
        table.add_column("Risk", justify="center", width=5)
        table.add_column("Conf", width=6)
        table.add_column("Entry $", justify="right")
        table.add_column("7D %", justify="right")
        table.add_column("30D %", justify="right")
        table.add_column("Status")

        for pred in predictions:
            status_style = {
                "correct": "green", "incorrect": "red",
                "mixed": "yellow", "pending": "dim",
            }.get(pred.outcome or "pending", "white")

            table.add_row(
                pred.timestamp.strftime("%Y-%m-%d"),
                pred.ticker,
                pred.direction.upper(),
                str(pred.risk_score),
                pred.confidence[:3].upper(),
                f"${pred.price_at_prediction:,.2f}",
                f"{pred.return_7d:+.1f}%" if pred.return_7d is not None else "—",
                f"{pred.return_30d:+.1f}%" if pred.return_30d is not None else "—",
                f"[{status_style}]{(pred.outcome or 'pending').upper()}[/{status_style}]",
            )

        console.print(table)
    finally:
        session.close()


def _calc_return(entry_price: float, current_price: float, direction: str) -> float:
    if direction == "short":
        return ((entry_price - current_price) / entry_price) * 100
    return ((current_price - entry_price) / entry_price) * 100


def _assess_outcome(pred: Prediction):
    """Classify prediction outcome based on 90-day return vs risk score expectation."""
    if pred.return_90d is None:
        return

    ret = pred.return_90d

    if pred.risk_score <= 3:
        # Low risk: expect moderate positive return with limited drawdown
        if ret > -5:
            pred.outcome = "correct"
        else:
            pred.outcome = "incorrect"
            pred.outcome_notes = f"Low-risk prediction had {ret:.1f}% return (expected >-5%)"
    elif pred.risk_score <= 6:
        # Medium risk: wider range acceptable
        if ret > -15:
            pred.outcome = "correct"
        else:
            pred.outcome = "incorrect"
    else:
        # High risk: just track — outcome is informational
        pred.outcome = "correct" if ret > 0 else "mixed"
