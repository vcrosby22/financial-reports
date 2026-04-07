"""Unit tests for crypto ↔ yfinance symbol mapping."""

from src.data.crypto import pair_to_yfinance_symbol


def test_pair_to_yfinance_usdt_pairs():
    assert pair_to_yfinance_symbol("BTC/USDT") == "BTC-USD"
    assert pair_to_yfinance_symbol("ETH/USDT") == "ETH-USD"
    assert pair_to_yfinance_symbol("SOL/USDT") == "SOL-USD"


def test_pair_to_yfinance_coinbase_mapped():
    assert pair_to_yfinance_symbol("BTC/USDT") == "BTC-USD"


def test_pair_to_yfinance_generic_usd():
    assert pair_to_yfinance_symbol("DOGE/USDT") == "DOGE-USD"
