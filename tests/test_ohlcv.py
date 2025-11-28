from finda.ohlcv_fetcher import fetch_unified_ohclv

API_KEY = "PKLNZTZ0TBT7WKZWSH4N"
SECRET_KEY = "iCH07K5wDECSRRzGECU6VDua2L3oUD2QlDhG1Rd8"

test_cases = [
    # Forex - Dukascopy, Non-US pairs, Sunday/Monday
    ("EUR/USD", "min5", "2025-09-07-22-00-00", "2025-09-08-02-00-00"),  # Sun night to Mon morning
    ("GBP/JPY", "hour1", "2025-09-06-23-00-00", "2025-09-07-02-00-00"), # Sat night to early Sun
    ("USD/JPY", "day1", "2025-09-06-00-00-00", "2025-09-09-00-00-00"),  # Sat to Tue

    # Crypto - Binance (always traded!)
    ("BTC/USDT", "min15", "2025-08-16-00-00-00", "2025-08-17-23-59-00"),  # Full weekend
    ("ETH/USDT", "hour1", "2025-09-06-00-00-00", "2025-09-08-00-00-00"),   # Sat to Mon

    # US Stocks - Yahoo/Alpaca, including market close
    ("AAPL", "day1", "2025-09-06-00-00-00", "2025-09-11-00-00-00"),      # Includes Sat/Sun
    ("MSFT", "hour1", "2025-09-05-09-00-00", "2025-09-09-16-00-00"),     # Fri to Tue (tests Fri close, weekend, Mon open)
    ("TSLA", "min5", "2025-09-05-15-55-00", "2025-09-05-16-35-00"),      # Fri close, after-hours

    # ETF - QQQ, weekend
    ("QQQ", "day1", "2025-09-06-00-00-00", "2025-09-09-00-00-00"),
]

for symbol, tf, start, end in test_cases:
    print(f"\n=== OHLCV {symbol} | {tf} | {start} ~ {end} ===")
    try:
        result = fetch_unified_ohclv(symbol, tf, start, end, API_KEY, SECRET_KEY)
        if result:
            o, h, l, c, v, t = result
            print(f"Bars: {len(o)}")
            print(f"Open: {[round(x, 4) for x in o[:3]]}")
            print(f"Volume: {v[:3]}")
            print(f"Time: {t[:3]}")
        else:
            print("No data returned.")
    except Exception as e:
        print(f"ERROR: {e}")
