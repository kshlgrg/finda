from finda.tick_fetcher import fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks

API_KEY = "PKLNZTZ0TBT7WKZWSH4N"
SECRET_KEY = "iCH07K5wDECSRRzGECU6VDua2L3oUD2QlDhG1Rd8"

test_cases = [
    # Forex (tick), including late night and weekend periods
    ("EUR/USD", "min1", "2025-09-07-23-00-00", "2025-09-08-01-00-00"),
    ("GBP/JPY", "min1", "2025-09-06-23-30-00", "2025-09-07-00-30-00"),
    # Crypto, always trading, including Sat/Sun
    ("BTC/USDT", "min1", "2025-08-16-22-00-00", "2025-08-17-01-00-00"),
    ("ETH/USDT", "min1", "2025-08-16-15-00-00", "2025-08-16-18-00-00"),
    # US stock tick, including closing minutes and pre-market
    ("AAPL", "min1", "2025-09-05-15-55-00", "2025-09-05-16-10-00"),  # Friday close/after-hours
    ("MSFT", "min1", "2025-09-08-08-00-00", "2025-09-08-09-35-00"),  # Monday pre-market
]

for symbol, tf, start, end in test_cases:
    print(f"\n=== TICK {symbol} | {start} ~ {end} ===")
    # Dukascopy
    try:
        b, a, bv, av, v, t = fetch_dukascopy_ticks(symbol, tf, start, end)
        print(f"Dukascopy ({symbol}) | Ticks: {len(b)}, Times: {t[:3]}")
    except Exception as e:
        print(f"Dukascopy ERROR: {e}")
    # Binance
    try:
        b, a, bv, av, v, t = fetch_binance_ticks(symbol, tf, start, end)
        print(f"Binance ({symbol}) | Ticks: {len(b)}, Times: {t[:3]}")
    except Exception as e:
        print(f"Binance ERROR: {e}")
    # Alpaca
    try:
        b, a, bv, av, v, t = fetch_alpaca_ticks(symbol, tf, start, end, API_KEY, SECRET_KEY)
        print(f"Alpaca ({symbol}) | Ticks: {len(b)}, Times: {t[:3]}")
    except Exception as e:
        print(f"Alpaca ERROR: {e}")
