import os
import sys
import time
import statistics
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from finda import fetch_unified_ohclv, fetch_unified_tick

# Load env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))
load_dotenv(env_path)

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

def run_stress_test():
    print("Starting Stress/Load Test...")

    # Configuration
    # We will simulate a "deep" fetch by requesting a longer timeframe
    # and multiple sequential requests to check for memory leaks or rate limits.

    scenarios = [
        {
            "name": "Long History OHLCV (Binance BTC/USDT 1m - 30 days)",
            "symbol": "BTC/USDT",
            "tf": "1m",
            "days": 30, # 30 days of 1m candles = 43,200 candles
            "type": "ohlcv"
        },
        {
            "name": "Heavy Tick Load (Binance BTC/USDT - 1 hour)",
            "symbol": "BTC/USDT",
            "tf": "tick",
            "duration_mins": 60, # 1 hour of ticks
            "type": "tick"
        }
    ]

    for s in scenarios:
        print(f"\n--- Scenario: {s['name']} ---")

        end_dt = datetime.now(timezone.utc) - timedelta(hours=12)

        if s['type'] == 'ohlcv':
            start_dt = end_dt - timedelta(days=s['days'])
        else:
            start_dt = end_dt - timedelta(minutes=s['duration_mins'])

        start_str = start_dt.strftime("%Y-%m-%d-%H-%M-%S")
        end_str = end_dt.strftime("%Y-%m-%d-%H-%M-%S")

        print(f"Fetching from {start_str} to {end_str}")

        t0 = time.time()
        try:
            if s['type'] == 'ohlcv':
                res = fetch_unified_ohclv(s['symbol'], s['tf'], start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                data_points = len(res[0]) if res else 0
            else:
                prov, res = fetch_unified_tick(s['symbol'], s['tf'], start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                data_points = len(res[0]) if res else 0

            elapsed = time.time() - t0

            print(f"Completed in {elapsed:.2f}s")
            print(f"Data Points: {data_points}")
            if data_points > 0:
                print(f"Rate: {data_points / elapsed:.2f} points/sec")
            else:
                print("FAILURE: No data returned")

        except Exception as e:
            print(f"FAILURE: Exception {e}")

if __name__ == "__main__":
    run_stress_test()
