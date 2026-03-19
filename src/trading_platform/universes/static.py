from __future__ import annotations

STATIC_UNIVERSES: dict[str, list[str]] = {
    "dow30": [
        "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS",
        "GS", "HD", "HON", "IBM", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK",
        "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WMT",
    ],
    "magnificent7": [
        "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA",
    ],
    "test_largecap": [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    ],
}
