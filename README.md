# trading-project

A Python trading research project for ingesting market data, generating features, and running simple strategy backtests.

## Current functionality

- Download OHLCV market data with Yahoo Finance
- Save raw data to Parquet
- Build simple features like returns and moving averages
- Run a simple SMA crossover backtest
- Save experiment outputs

## Project structure

```text
trading-project/
├─ data/
│  ├─ raw/
│  └─ features/
├─ src/
│  └─ trading_platform/
│     ├─ backtests/
│     ├─ cli/
│     ├─ data/
│     ├─ experiments/
│     ├─ features/
│     └─ settings.py
├─ .gitignore
└─ pyproject.toml