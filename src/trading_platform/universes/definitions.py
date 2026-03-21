from __future__ import annotations

import json
from pathlib import Path


def _normalized(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        cleaned = str(symbol).strip().upper()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def load_universe_definitions() -> dict[str, list[str]]:
    return {
        "dow30": _normalized([
            "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS",
            "GS", "HD", "HON", "IBM", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK",
            "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WMT",
        ]),
        "magnificent7": _normalized([
            "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA",
        ]),
        "test_largecap": _normalized([
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
        ]),
        "debug_liquid10": _normalized([
            "AAPL", "MSFT", "NVDA", "AMZN", "META",
            "GOOGL", "JPM", "XOM", "UNH", "COST",
        ]),
        "nasdaq100": _normalized([
            "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD", "AMGN",
            "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AXON", "AZN", "BIIB", "BKNG",
            "CDNS", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO", "CSX", "CTAS",
            "CTSH", "DASH", "DDOG", "DXCM", "EA", "EXC", "FAST", "FTNT", "GEHC", "GFS",
            "GILD", "GOOG", "GOOGL", "HON", "IDXX", "INTC", "INTU", "ISRG", "KDP", "KHC",
            "KLAC", "LIN", "LRCX", "LULU", "MAR", "MCHP", "MDLZ", "MELI", "META", "MNST",
            "MRVL", "MSFT", "MSTR", "MU", "NFLX", "NVDA", "NXPI", "ODFL", "ON", "ORLY",
            "PANW", "PAYX", "PCAR", "PDD", "PEP", "PYPL", "QCOM", "REGN", "ROP", "ROST",
            "SBUX", "SNPS", "TEAM", "TMUS", "TSLA", "TTD", "TTWO", "TXN", "VRSK", "VRTX",
            "WBD", "WDAY", "XEL", "ZS",
        ]),
        "liquid_top_100": _normalized([
            "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK.B", "JPM",
            "LLY", "V", "XOM", "UNH", "MA", "COST", "JNJ", "HD", "PG", "ABBV",
            "MRK", "BAC", "KO", "CVX", "WMT", "CRM", "NFLX", "AMD", "PEP", "ADBE",
            "LIN", "TMO", "ORCL", "MCD", "CSCO", "ABT", "ACN", "DHR", "TMUS", "QCOM",
            "INTU", "CMCSA", "AMGN", "TXN", "PFE", "IBM", "PM", "DIS", "CAT", "AMAT",
            "NOW", "GE", "INTC", "BKNG", "ISRG", "GS", "UNP", "SPGI", "AXP", "RTX",
            "BLK", "LOW", "UBER", "MS", "SCHW", "PLTR", "PANW", "VRTX", "LRCX", "ADI",
            "TJX", "NEE", "SYK", "C", "MU", "MMC", "CB", "DE", "SO", "ETN",
            "MDT", "GILD", "ADP", "ANET", "HON", "PGR", "MO", "AMT", "DUK", "CI",
            "COP", "BMY", "BA", "NKE", "ELV", "SLB", "MDLZ", "REGN", "CME", "PYPL",
        ]),
        "sp500": _normalized([
            "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META", "BRK.B", "TSLA", "AVGO",
            "LLY", "JPM", "V", "XOM", "UNH", "MA", "COST", "JNJ", "HD", "PG",
            "ABBV", "MRK", "BAC", "KO", "CVX", "WMT", "CRM", "NFLX", "AMD", "PEP",
            "ADBE", "LIN", "TMO", "ORCL", "MCD", "CSCO", "ABT", "ACN", "DHR", "TMUS",
            "QCOM", "INTU", "CMCSA", "AMGN", "TXN", "PFE", "IBM", "DIS", "CAT", "AMAT",
            "NOW", "GE", "INTC", "BKNG", "ISRG", "GS", "UNP", "SPGI", "AXP", "RTX",
            "BLK", "LOW", "UBER", "MS", "SCHW", "PANW", "VRTX", "LRCX", "ADI", "TJX",
            "NEE", "SYK", "C", "MU", "MMC", "CB", "DE", "SO", "ETN", "MDT",
            "GILD", "ADP", "ANET", "HON", "PGR", "MO", "AMT", "DUK", "CI", "COP",
            "BMY", "BA", "NKE", "ELV", "SLB", "MDLZ", "REGN", "CME", "PYPL", "AON",
            "ICE", "CL", "MCK", "PLD", "ITW", "APD", "SHW", "PNC", "TT", "APH",
            "WM", "FDX", "EW", "EMR", "NSC", "GM", "EOG", "PSX", "MCO", "MAR",
            "ROP", "AEP", "FIS", "FCX", "ORLY", "ADM", "NOC", "AZO", "GIS", "CMG",
            "SBUX", "CDNS", "SNPS", "ADSK", "KLAC", "MPC", "PAYX", "PH", "WELL", "ROP",
            "AJG", "HCA", "USB", "OXY", "MET", "AFL", "TRV", "AIG", "PSA", "PCAR",
            "BK", "KMB", "MSI", "D", "CTAS", "A", "ROST", "NXPI", "MNST", "EXC",
            "IDXX", "CHTR", "ALL", "TDG", "FTNT", "SPG", "TEL", "ODFL", "AMP", "YUM",
            "BIIB", "F", "KMI", "PRU", "HLT", "VLO", "O", "SYY", "ROK", "FAST",
            "CTSH", "CSX", "NEM", "CCI", "XEL", "DOW", "CPRT", "RSG", "ED", "TRMB",
            "MCHP", "WMB", "TFC", "PEG", "VICI", "WEC", "KR", "EA", "LEN", "AME",
            "ECL", "HPQ", "BKR", "LHX", "PXD", "AEE", "DLR", "ILMN", "DD", "HAL",
            "ANSS", "DFS", "DVN", "MPWR", "FANG", "KHC", "TSCO", "SBAC", "STZ", "CTVA",
            "RMD", "GWW", "WTW", "DAL", "EBAY", "EFX", "FITB", "KEYS", "HIG", "IRM",
            "AVB", "GLW", "AWK", "VRSK", "WST", "DTE", "EIX", "BRO", "IFF", "MLM",
            "FICO", "MTD", "PPL", "ES", "FSLR", "ALGN", "FE", "BR", "CARR", "NDAQ",
            "ON", "VRSN", "STE", "TTWO", "CMS", "HUM", "GPC", "EL", "RJF", "AXON",
            "CHD", "CNP", "AIZ", "ATO", "MKC", "PPG", "SYF", "RF", "ULTA", "CINF",
            "WRB", "ETR", "NTRS", "DHI", "INVH", "ESS", "COF", "WAT", "HBAN", "LUV",
            "TER", "PFG", "ARE", "LYB", "PTC", "HOLX", "IEX", "JCI", "PKG", "TYL",
            "LH", "EXR", "L", "TXT", "STT", "MAA", "OMC", "TRGP", "DG", "CLX",
            "EPAM", "K", "SIVB", "MAS", "JBHT", "MOH", "J", "GEN", "BALL", "IP",
            "CF", "CBOE", "ZBRA", "NVR", "SWK", "VTR", "PEAK", "UDR", "CHRW", "AKAM",
            "HPE", "LNT", "EVRG", "MRO", "POOL", "DPZ", "ABNB", "BBY", "CFG", "KEY",
            "JKHY", "CAG", "DRI", "MKTX", "REG", "INCY", "SEDG", "SWKS", "EXPE", "AES",
            "MOS", "APA", "CAH", "CPB", "HSY", "HRL", "JKHY", "BAX", "SOLV", "UHS",
            "AAL", "NWSA", "NWS", "PARA", "FOXA", "FOX", "SJM", "VMC", "RL", "LULU",
            "KMX", "ETSY", "WBD", "GEN", "DAY", "DOC", "GNRC", "ZBH", "PAYC", "QRVO",
            "HAS", "NDSN", "FRT", "NI", "DGX", "AVY", "XRAY", "PNR", "BIO", "TECH",
            "TFX", "LW", "HST", "CE", "BWA", "STX", "AAP", "WY", "IVZ", "AOS",
            "PHM", "BEN", "NCLH", "RCL", "CZR", "WYNN", "TPR", "LKQ", "FDS", "CPT",
            "KIM", "MGM", "PODD", "ALB", "BBWI", "IPG", "NWS", "HSIC", "GNW", "RVTY",
            "CRL", "JNPR", "LDOS", "CDW", "HUBB", "WDC", "NRG", "MTCH", "PAYX", "VTRS",
            "PNW", "AIZ", "XRAY", "TTWO", "MRNA", "ENPH", "KEYS", "ALLE", "FLT", "BRX",
            "AIZ", "RE", "COTY", "KDP", "ETSY", "FMC", "TER", "ZION", "HBAN", "CFG",
            "RHI", "TXT", "LII", "APTV", "GEN", "DOV", "CPAY", "CTLT", "RVTY", "SNA",
            "WBA", "NUE", "STLD", "PKI", "CMA", "PNC", "OKE", "MHK", "NWSA", "IFF",
            "HSY", "ROL", "EMN", "HII", "LVS", "UAL", "MKTX", "AER", "SJM", "KIM",
            "COO", "CF", "CNP", "DVA", "BXP", "CPT", "EXPD", "FFIV", "AKAM", "VFC",
            "SRE", "WAB", "CHKP", "DOCU", "SNV", "MOS", "PKI", "NTRS", "PWR", "ZTS",
        ]),
    }


UNIVERSE_DEFINITIONS = load_universe_definitions()


def export_universe_definitions(path: str | Path) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(UNIVERSE_DEFINITIONS, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path
