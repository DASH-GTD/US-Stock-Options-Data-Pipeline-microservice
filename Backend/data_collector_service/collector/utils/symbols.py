import os

# SYMBOLS_SETS = [
#     ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "BRK-B", "TSLA", "JPM", "WMT", "UNH", "V", "MA", "XOM", "LLY", "PG", "HD", "KO", "COST", "ADBE", "BAC", "PEP", "CSCO"],
#     ["NFLX", "CRM", "ORCL", "INTC", "AMD", "TMO", "MCD", "ABT", "CVX", "DIS", "WFC", "IBM", "QCOM", "CAT", "GS", "AMGN", "DHR", "NKE", "LOW", "INTU", "TXN", "UPS", "CMCSA", "SPGI"],
#     ["HON", "RTX", "BA", "C", "PFE", "T", "GE", "MMM", "DE", "LMT", "SCHW", "MDT", "CB", "ELV", "BLK", "AXP", "CI", "SBUX", "BMY", "GILD", "SYK", "ADP", "PLD", "MMC"],
#     ["MO", "COP", "TJX", "NEE", "SO", "DUK", "ZTS", "EOG", "SLB", "VRTX", "REGN", "BSX", "ADI", "KLAC", "PANW", "AMAT", "LRCX", "CSX", "NSC", "ITW", "SHW", "EMR", "AON", "FDX"],
#     ["ECL", "TGT", "MCK", "USB", "CME", "PNC", "MAR", "PH", "ROP", "MCO", "AFL", "TRV", "PSX", "OXY", "MET", "AIG", "EW", "HUM", "D", "AEP", "STZ", "KMB", "GIS", "YUM"]
# ]

# SYMBOLS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "BRK-B", "TSLA", "JPM", "WMT", "UNH", "V", "MA", "XOM", "LLY", "PG", "HD", "KO", "COST", "ADBE", "BAC", "PEP", "CSCO",
#     "NFLX", "CRM", "ORCL", "INTC", "AMD", "TMO", "MCD", "ABT", "CVX", "DIS", "WFC", "IBM", "QCOM", "CAT", "GS", "AMGN", "DHR", "NKE", "LOW", "INTU", "TXN", "UPS", "CMCSA", "SPGI",
#     "HON", "RTX", "BA", "C", "PFE", "T", "GE", "MMM", "DE", "LMT", "SCHW", "MDT", "CB", "ELV", "BLK", "AXP", "CI", "SBUX", "BMY", "GILD", "SYK", "ADP", "PLD", "MMC",
#     "MO", "COP", "TJX", "NEE", "SO", "DUK", "ZTS", "EOG", "SLB", "VRTX", "REGN", "BSX", "ADI", "KLAC", "PANW", "AMAT", "LRCX", "CSX", "NSC", "ITW", "SHW", "EMR", "AON", "FDX",
#     "ECL", "TGT", "MCK", "USB", "CME", "PNC", "MAR", "PH", "ROP", "MCO", "AFL", "TRV", "PSX", "OXY", "MET", "AIG", "EW", "HUM", "D", "AEP", "STZ", "KMB", "GIS", "YUM"
# ]


SYMBOLS = ['MMM']

SYMBOL_SETS = [
    ["MMM"], 
    ["AAPL"], 
    ["DELL"], 
    ["AOS"],
    ["BRO"]
    ]

API_KEYS = [
    os.getenv("TWELVE_DATA_API_1"), 
    os.getenv("TWELVE_DATA_API_2"), 
    os.getenv("TWELVE_DATA_API_3"), 
    os.getenv("TWELVE_DATA_API_4"), 
    os.getenv("TWELVE_DATA_API_5")
]