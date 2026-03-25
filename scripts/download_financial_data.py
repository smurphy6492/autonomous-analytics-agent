"""Download financial market data from Yahoo Finance.

Produces 2 CSV files in data/raw/finance/:
  - daily_prices.csv: 5 years of daily OHLCV for top 50 S&P 500 stocks + SPY benchmark
  - tickers.csv: ticker metadata (sector, industry, market cap bucket)

Top 50 by market cap as of early 2026. Covers 11 GICS sectors.
"""

from __future__ import annotations

import csv
from pathlib import Path

import yfinance as yf

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "finance"

# Top 50 S&P 500 by market cap (approximate, early 2026)
TICKERS = {
    # Technology
    "AAPL": {"sector": "Technology", "industry": "Consumer Electronics"},
    "MSFT": {"sector": "Technology", "industry": "Software"},
    "NVDA": {"sector": "Technology", "industry": "Semiconductors"},
    "AVGO": {"sector": "Technology", "industry": "Semiconductors"},
    "ORCL": {"sector": "Technology", "industry": "Software"},
    "CRM": {"sector": "Technology", "industry": "Software"},
    "AMD": {"sector": "Technology", "industry": "Semiconductors"},
    "ADBE": {"sector": "Technology", "industry": "Software"},
    "CSCO": {"sector": "Technology", "industry": "Networking"},
    "ACN": {"sector": "Technology", "industry": "IT Services"},
    # Communication Services
    "GOOG": {"sector": "Communication Services", "industry": "Internet"},
    "META": {"sector": "Communication Services", "industry": "Social Media"},
    "NFLX": {"sector": "Communication Services", "industry": "Streaming"},
    # Consumer Discretionary
    "AMZN": {"sector": "Consumer Discretionary", "industry": "E-Commerce"},
    "TSLA": {"sector": "Consumer Discretionary", "industry": "Automotive"},
    "HD": {"sector": "Consumer Discretionary", "industry": "Home Improvement"},
    "MCD": {"sector": "Consumer Discretionary", "industry": "Restaurants"},
    "LOW": {"sector": "Consumer Discretionary", "industry": "Home Improvement"},
    # Consumer Staples
    "WMT": {"sector": "Consumer Staples", "industry": "Retail"},
    "COST": {"sector": "Consumer Staples", "industry": "Retail"},
    "PG": {"sector": "Consumer Staples", "industry": "Household Products"},
    "KO": {"sector": "Consumer Staples", "industry": "Beverages"},
    "PEP": {"sector": "Consumer Staples", "industry": "Beverages"},
    # Financials
    "BRK-B": {"sector": "Financials", "industry": "Conglomerate"},
    "JPM": {"sector": "Financials", "industry": "Banks"},
    "V": {"sector": "Financials", "industry": "Payments"},
    "MA": {"sector": "Financials", "industry": "Payments"},
    "BAC": {"sector": "Financials", "industry": "Banks"},
    "GS": {"sector": "Financials", "industry": "Investment Banking"},
    # Healthcare
    "LLY": {"sector": "Healthcare", "industry": "Pharmaceuticals"},
    "UNH": {"sector": "Healthcare", "industry": "Insurance"},
    "JNJ": {"sector": "Healthcare", "industry": "Pharmaceuticals"},
    "ABBV": {"sector": "Healthcare", "industry": "Pharmaceuticals"},
    "MRK": {"sector": "Healthcare", "industry": "Pharmaceuticals"},
    "TMO": {"sector": "Healthcare", "industry": "Life Sciences"},
    "ABT": {"sector": "Healthcare", "industry": "Medical Devices"},
    # Energy
    "XOM": {"sector": "Energy", "industry": "Oil & Gas"},
    "CVX": {"sector": "Energy", "industry": "Oil & Gas"},
    # Industrials
    "GE": {"sector": "Industrials", "industry": "Aerospace"},
    "CAT": {"sector": "Industrials", "industry": "Machinery"},
    "UNP": {"sector": "Industrials", "industry": "Railroads"},
    "RTX": {"sector": "Industrials", "industry": "Aerospace & Defense"},
    "HON": {"sector": "Industrials", "industry": "Conglomerate"},
    "UPS": {"sector": "Industrials", "industry": "Logistics"},
    # Real Estate
    "PLD": {"sector": "Real Estate", "industry": "REITs"},
    "AMT": {"sector": "Real Estate", "industry": "REITs"},
    # Utilities
    "NEE": {"sector": "Utilities", "industry": "Electric Utilities"},
    # Materials
    "LIN": {"sector": "Materials", "industry": "Industrial Gases"},
    "APD": {"sector": "Materials", "industry": "Industrial Gases"},
    # Benchmark
    "SPY": {"sector": "Benchmark", "industry": "S&P 500 ETF"},
}

PERIOD = "5y"


def download_prices() -> None:
    ticker_list = list(TICKERS.keys())
    print(f"Downloading {PERIOD} of daily data for {len(ticker_list)} tickers...")

    data = yf.download(ticker_list, period=PERIOD, group_by="ticker", auto_adjust=True)

    rows = []
    for ticker in ticker_list:
        try:
            if len(ticker_list) == 1:
                df = data
            else:
                df = data[ticker]
            df = df.dropna(subset=["Close"])
            for idx, row in df.iterrows():
                rows.append({
                    "date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx),
                    "ticker": ticker,
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]),
                })
        except (KeyError, TypeError) as e:
            print(f"  Warning: skipping {ticker}: {e}")

    path = OUTPUT_DIR / "daily_prices.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "ticker", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  daily_prices.csv: {len(rows)} rows")


def write_ticker_metadata() -> None:
    rows = []
    for ticker, meta in TICKERS.items():
        rows.append({
            "ticker": ticker,
            "sector": meta["sector"],
            "industry": meta["industry"],
        })

    path = OUTPUT_DIR / "tickers.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "sector", "industry"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"  tickers.csv: {len(rows)} rows")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Downloading financial dataset...")
    write_ticker_metadata()
    download_prices()
    print(f"\nDone. Files written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
