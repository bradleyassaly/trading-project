from trading_platform.data.ingest import ingest_symbol

if __name__ == "__main__":
    path = ingest_symbol("SPY")
    print("saved to", path)