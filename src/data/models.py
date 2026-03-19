from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class MarketSnapshot(Base):
    """Point-in-time capture of a ticker's market data."""

    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), nullable=False, index=True)
    asset_type = Column(String(20), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    price = Column(Float)
    open_price = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Float)
    market_cap = Column(Float)

    change_pct_1d = Column(Float)
    change_pct_1w = Column(Float)
    change_pct_1m = Column(Float)

    pe_ratio = Column(Float)
    pb_ratio = Column(Float)
    dividend_yield = Column(Float)
    fifty_day_ma = Column(Float)
    two_hundred_day_ma = Column(Float)
    rsi_14 = Column(Float)

    def __repr__(self):
        return f"<MarketSnapshot {self.ticker} @ {self.price} ({self.timestamp})>"


class AnalysisReport(Base):
    """AI-generated analysis reports."""

    __tablename__ = "analysis_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_type = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    summary = Column(Text, nullable=False)
    full_report = Column(Text, nullable=False)
    risk_level = Column(String(20))
    confidence = Column(String(20))
    key_findings = Column(Text)


class Alert(Base):
    """Generated alerts that may need notification."""

    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    alert_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    ticker = Column(String(20))
    message = Column(Text, nullable=False)
    notified = Column(Integer, default=0)


class Prediction(Base):
    """Logged predictions for accuracy tracking over time.

    Every opportunity surfaced by the agent gets logged here.
    A daily job checks price at 7, 30, 60, 90 days to track
    whether the risk score matched reality.
    """

    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    ticker = Column(String(20), nullable=False, index=True)
    direction = Column(String(10), nullable=False)  # long, short
    risk_score = Column(Integer, nullable=False)  # 1-10
    confidence = Column(String(20), nullable=False)  # low, medium, high
    time_horizon = Column(String(20))  # very_short, short, medium, long
    reasoning = Column(Text)
    price_at_prediction = Column(Float, nullable=False)

    price_7d = Column(Float)
    price_30d = Column(Float)
    price_60d = Column(Float)
    price_90d = Column(Float)
    return_7d = Column(Float)
    return_30d = Column(Float)
    return_60d = Column(Float)
    return_90d = Column(Float)
    max_drawdown_30d = Column(Float)

    outcome = Column(String(20))  # pending, correct, incorrect, mixed
    outcome_notes = Column(Text)
