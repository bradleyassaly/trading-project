import pandas as pd
import yfinance as yf

from trading_platform.settings import RAW_DATA_DIR


def ingest_symbol(symbol: str, start: str = "2010-01-01"):
    df = yf.download(symbol, start=start, progress=False, auto_adjust=False)

    if df.empty:
        raise ValueError(f"No data returned for {symbol}")

    # Flatten MultiIndex columns from yfinance if present
    if isinstance(df.columns, pd.MultiIndex):
        found = False
        for level in range(df.columns.nlevels):
            vals = set(df.columns.get_level_values(level))
            if {"Open", "High", "Low", "Close", "Volume"}.issubset(vals):
                df.columns = df.columns.get_level_values(level)
                found = True
                break

        if not found:
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # Normalize names
    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    required = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Actual columns: {list(df.columns)}")

    df["symbol"] = symbol
    df["timestamp"] = pd.to_datetime(df.index)

    df = df.reset_index(drop=True)
    df = df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]

    path = RAW_DATA_DIR / f"{symbol}.parquet"

    df.to_parquet(path, index=False)

    return path