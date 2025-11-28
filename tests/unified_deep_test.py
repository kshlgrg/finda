import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Add parent of 'finda' directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    from finda.ohlcv_fetcher import fetch_unified_ohclv
    from finda.tick_fetcher import fetch_unified_tick
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
    from ohlcv_fetcher import fetch_unified_ohclv
    from tick_fetcher import fetch_unified_tick

# Load env
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../.env'))
load_dotenv(env_path)

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

print(f"Loaded Keys - Key: {'Found' if ALPACA_API_KEY else 'Missing'}")

def run_deep_tests():
    print("Starting Deep Unified Tests...")
    
    # Test Cases
    # 1. Standard Timeframes (1h, 1d)
    # 2. Crypto via Alpaca (BTC/USD)
    # 3. Stock via Alpaca (AAPL)
    # 4. Forex via Dukascopy (EUR/USD)
    
    tests = [
        {"name": "Stock OHLCV (Alpaca/Unified)", "symbol": "AAPL", "tf": "1d", "days": 5},
        {"name": "Crypto OHLCV (Alpaca/Unified)", "symbol": "BTC/USD", "tf": "1h", "days": 1},
        {"name": "Forex OHLCV (Dukascopy/Unified)", "symbol": "EUR/USD", "tf": "1h", "days": 2},
        {"name": "Crypto Tick (Binance/Unified)", "symbol": "BTC/USDT", "tf": "tick", "days": 0.001},
        {"name": "Forex Tick (Dukascopy/Unified)", "symbol": "GBP/USD", "tf": "tick", "days": 0.01},
    ]
    
    results = []
    
    for t in tests:
        print(f"\nRunning: {t['name']}")
        symbol = t['symbol']
        tf = t['tf']
        days = t['days']
        
        # Use UTC for Alpaca compatibility
        end_dt = datetime.now(timezone.utc) - timedelta(minutes=20)
        start_dt = end_dt - timedelta(days=days)
        
        start_str = start_dt.strftime("%Y-%m-%d-%H-%M-%S")
        end_str = end_dt.strftime("%Y-%m-%d-%H-%M-%S")
        
        status = "PASS"
        details = ""
        
        try:
            if tf == "tick":
                prov, res = fetch_unified_tick(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                if res:
                    b, a, bv, av, rv, times = res
                    details = f"Provider: {prov}, Ticks: {len(times)}"
                    if len(times) == 0:
                        status = "FAIL"
                        details = "Empty tick list"
                else:
                    status = "FAIL"
                    details = "None returned"
            else:
                res = fetch_unified_ohclv(symbol, tf, start_str, end_str, ALPACA_API_KEY, ALPACA_SECRET_KEY)
                if res:
                    o, h, l, c, v, times = res
                    details = f"Rows: {len(times)}"
                    if len(times) > 0:
                        details += f", First: {times[0]}, Last: {times[-1]}"
                    else:
                        status = "FAIL"
                        details = "Empty OHLCV list"
                else:
                    status = "FAIL"
                    details = "None returned"
                    
        except Exception as e:
            status = "FAIL"
            details = str(e)
            
        print(f"  -> {status} ({details})")
        results.append({"test": t['name'], "status": status, "details": details})

    # Generate Report
    with open(os.path.join(os.path.dirname(__file__), "deep_test_report.md"), "w") as f:
        f.write("# Deep Unified Test Report\n\n")
        f.write("| Test | Status | Details |\n|---|---|---|\n")
        for r in results:
            f.write(f"| {r['test']} | {r['status']} | {r['details']} |\n")

if __name__ == "__main__":
    run_deep_tests()
