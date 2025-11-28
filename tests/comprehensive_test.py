import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add parent of 'finda' directory to path to allow 'from finda import ...'
# current file is finda/tests/comprehensive_test.py -> ../../ is the parent of finda
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    from finda.ohlcv_fetcher import fetch_unified_ohclv, fetch_dukascopy_ohclv, fetch_binance_ohclv, fetch_alpaca_ohclv
    from finda.tick_fetcher import fetch_unified_tick, fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks
except ImportError:
    # Fallback: try adding just the parent directory if we are running from within finda or if structure is different
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
    from ohlcv_fetcher import fetch_unified_ohclv, fetch_dukascopy_ohclv, fetch_binance_ohclv, fetch_alpaca_ohclv
    from tick_fetcher import fetch_unified_tick, fetch_dukascopy_ticks, fetch_binance_ticks, fetch_alpaca_ticks

# Load env from finda/.env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))
load_dotenv(env_path)

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

print(f"Loaded Keys - Key: {'Found' if ALPACA_API_KEY else 'Missing'}, Secret: {'Found' if ALPACA_SECRET_KEY else 'Missing'}")

# Test Configuration
TEST_CASES = [
    # Dukascopy
    {"provider": "dukascopy", "type": "ohlcv", "symbol": "EUR/USD", "tf": "hour1", "days_back": 2},
    {"provider": "dukascopy", "type": "tick", "symbol": "EUR/USD", "tf": "tick", "days_back": 0.04}, # ~1 hour
    
    # Binance
    {"provider": "binance", "type": "ohlcv", "symbol": "BTC/USDT", "tf": "hour1", "days_back": 2},
    {"provider": "binance", "type": "tick", "symbol": "BTC/USDT", "tf": "tick", "days_back": 0.001}, # Very short for ticks to avoid huge download
    
    # Alpaca
    {"provider": "alpaca", "type": "ohlcv", "symbol": "AAPL", "tf": "day1", "days_back": 5},
    {"provider": "alpaca", "type": "ohlcv", "symbol": "BTC/USD", "tf": "hour1", "days_back": 1},
    {"provider": "alpaca", "type": "tick", "symbol": "AAPL", "tf": "tick", "days_back": 1}, # Increased to 1 day to ensure we get data outside 15m window if needed, or just enough history
    
    # Unified
    {"provider": "unified", "type": "ohlcv", "symbol": "BTC/USDT", "tf": "hour1", "days_back": 1}, 
    {"provider": "unified", "type": "tick", "symbol": "EUR/USD", "tf": "tick", "days_back": 0.01}, 
]

results = []

def run_tests():
    print("Starting Comprehensive Tests...")
    
    for test in TEST_CASES:
        provider = test["provider"]
        data_type = test["type"]
        symbol = test["symbol"]
        tf = test["tf"]
        days_back = test["days_back"]
        
        # End time 20 mins ago (UTC) to avoid Alpaca 15-min delay and timezone issues
        end_dt = datetime.utcnow() - timedelta(minutes=20)
        start_dt = end_dt - timedelta(days=days_back)
        
        # Format dates as expected by the fetchers (YYYY-MM-DD-HH-MM-SS)
        start_str = start_dt.strftime("%Y-%m-%d-%H-%M-%S")
        end_str = end_dt.strftime("%Y-%m-%d-%H-%M-%S")
        
        print(f"Testing {provider} {data_type} for {symbol}...")
        
        status = "PASS"
        error_msg = ""
        data_summary = ""
        
        try:
            res = None
            if data_type == "ohlcv":
                if provider == "dukascopy":
                    res = fetch_dukascopy_ohclv(symbol, tf, start_str, end_str)
                elif provider == "binance":
                    res = fetch_binance_ohclv(symbol, tf, start_str, end_str)
                elif provider == "alpaca":
                    res = fetch_alpaca_ohclv(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                elif provider == "unified":
                    res = fetch_unified_ohclv(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                
                if res:
                    opens, highs, lows, closes, volumes, times = res
                    data_summary = f"Rows: {len(times)}"
                    if len(times) > 0:
                        data_summary += f", First: {times[0]}, Last: {times[-1]}"
                    else:
                        status = "FAIL"
                        error_msg = "Empty data returned"
                else:
                    status = "FAIL"
                    error_msg = "None returned"

            elif data_type == "tick":
                if provider == "dukascopy":
                    res = fetch_dukascopy_ticks(symbol, tf, start_str, end_str)
                elif provider == "binance":
                    res = fetch_binance_ticks(symbol, tf, start_str, end_str)
                elif provider == "alpaca":
                    res = fetch_alpaca_ticks(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                elif provider == "unified":
                    prov_used, res = fetch_unified_tick(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                    if prov_used:
                         data_summary = f"Provider Used: {prov_used}. "
                    else:
                        status = "FAIL"
                        error_msg = "Unified fetch failed to find provider"
                        res = None

                if res:
                    b, a, bv, av, rv, t = res
                    data_summary += f"Ticks: {len(t)}"
                    if len(t) > 0:
                         data_summary += f", First: {t[0]}, Last: {t[-1]}"
                    else:
                        status = "FAIL"
                        error_msg = "Empty tick data"
                elif status != "FAIL":
                    status = "FAIL"
                    error_msg = "None returned"

        except Exception as e:
            status = "FAIL"
            error_msg = str(e)
            # print(f"Error details: {e}")

        results.append({
            "provider": provider,
            "type": data_type,
            "symbol": symbol,
            "tf": tf,
            "status": status,
            "details": error_msg if status == "FAIL" else data_summary
        })
        print(f"  -> {status} ({error_msg if status == 'FAIL' else data_summary})")

    generate_report()

def generate_report():
    report_path = os.path.join(os.path.dirname(__file__), "test_report.md")
    with open(report_path, "w") as f:
        f.write("# Finda Comprehensive Test Report\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("| Provider | Type | Symbol | Timeframe | Status | Details |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in results:
            f.write(f"| {r['provider']} | {r['type']} | {r['symbol']} | {r['tf']} | {r['status']} | {r['details']} |\n")
    
    print(f"\nReport generated at {report_path}")

if __name__ == "__main__":
    run_tests()
