import ccxt
import pandas as pd
from datetime import datetime
import re

def parse_tf(tf):
    # Try unit+num (e.g. min1)
    match = re.match(r"([a-zA-Z]+)(\d+)", tf.strip().lower())
    if match:
        return match.group(1), match.group(2)
    # Try num+unit (e.g. 1min)
    match = re.match(r"(\d+)([a-zA-Z]+)", tf.strip().lower())
    if match:
        return match.group(2), match.group(1)
    return None, None

def user_to_dukascopy_tf(tf):
    unit, num = parse_tf(tf)
    units = {
        "min": "MIN", "m": "MIN",
        "hour": "HOUR", "h": "HOUR",
        "day": "DAY", "d": "DAY",
        "week": "WEEK", "w": "WEEK",
        "month": "MONTH", "mo": "MONTH",
        "year": "YEAR", "y": "YEAR",
        "sec": "SEC", "s": "SEC"
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Dukascopy timeframe: '{tf}'")

def user_to_binance_tf(tf):
    unit, num = parse_tf(tf)
    units = {
        "min": "m", "m": "m",
        "hour": "h", "h": "h",
        "day": "d", "d": "d",
        "week": "w", "w": "w",
        "month": "M", "mo": "M",
        "sec": "s", "s": "s"
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Binance timeframe: '{tf}'")

def user_to_alpaca_tf(tf):
    unit, num = parse_tf(tf)
    units = {
        "min": "Min", "m": "Min",
        "hour": "Hour", "h": "Hour",
        "day": "Day", "d": "Day",
        "week": "Week", "w": "Week",
    }
    if unit:
        for k, v in units.items():
            if unit.startswith(k): return f"{num}{v}"
    raise ValueError(f"Unrecognized Alpaca timeframe: '{tf}'")

def user_to_dt(s, as_type='datetime'):
    parts = [int(p) for p in s.split('-')]
    while len(parts) < 6: parts.append(0)
    dt = datetime(*parts)
    return dt if as_type == 'datetime' else dt.strftime("%Y-%m-%dT%H:%M:%S")

def fetch_dukascopy_ohclv(symbol, user_tf, user_start, user_end):
    from dukascopy_python import fetch, OFFER_SIDE_BID
    symbol = symbol.strip().upper()
    tf = user_to_dukascopy_tf(user_tf)
    start = user_to_dt(user_start, 'datetime')
    end = user_to_dt(user_end, 'datetime')
    df = fetch(symbol, tf, OFFER_SIDE_BID, start=start, end=end)
    if df is None or df.empty: raise ValueError(f"No Dukascopy data for {symbol}")
    opens, highs, lows, closes = df["open"].tolist(), df["high"].tolist(), df["low"].tolist(), df["close"].tolist()
    volumes = df["volume"].tolist() if "volume" in df else [0] * len(df)
    times = list(df.index.to_pydatetime())
    return opens, highs, lows, closes, volumes, times

def fetch_binance_ohclv(symbol, user_tf, user_start, user_end):
    exchange = ccxt.binance({'enableRateLimit': True})
    symbol = symbol.strip().upper()
    binance_symbol = symbol.replace("/", "")
    tf = user_to_binance_tf(user_tf)
    since_str, end_str = user_to_dt(user_start, 'iso'), user_to_dt(user_end, 'iso')
    since = int(datetime.fromisoformat(since_str).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(end_str).timestamp() * 1000)
    limit, all_ohlcv = 1000, []
    while since < end_ms:
        ohlcv = exchange.fetch_ohlcv(binance_symbol, tf, since, limit)
        if not ohlcv: break
        filtered = [c for c in ohlcv if c[0] <= end_ms]
        all_ohlcv.extend(filtered)
        if len(ohlcv) < limit: break
        since = ohlcv[-1][0] + 1
    if not all_ohlcv: raise ValueError(f"No Binance data for {symbol}")
    timestamps, opens, highs, lows, closes, volumes = zip(*all_ohlcv)
    times = [datetime.fromtimestamp(t / 1000) for t in timestamps]
    return list(opens), list(highs), list(lows), list(closes), list(volumes), times

def fetch_alpaca_ohclv(symbol, user_tf, user_start, user_end, api_key, secret_key):
    from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
    from alpaca.data.timeframe import TimeFrame
    symbol = symbol.strip().upper()
    tf = user_to_alpaca_tf(user_tf)
    tf_map = {'1Min': TimeFrame.Minute, '5Min': TimeFrame(5, TimeFrame.Minute), '15Min': TimeFrame(15, TimeFrame.Minute), '30Min': TimeFrame(30, TimeFrame.Minute), '1Hour': TimeFrame.Hour, '1Day': TimeFrame.Day, '1Week': TimeFrame.Week}
    start = user_to_dt(user_start, 'iso')
    end = user_to_dt(user_end, 'iso')
    
    # Check if crypto
    is_crypto = '/' in symbol or symbol in ['BTCUSD', 'ETHUSD'] # Simple heuristic
    
    if is_crypto:
        client = CryptoHistoricalDataClient(api_key, secret_key) # Crypto client doesn't strictly need keys for some pairs but good to pass
        # Alpaca Crypto symbols often like BTC/USD
        request = CryptoBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf_map[tf],
            start=datetime.fromisoformat(start), end=datetime.fromisoformat(end)
        )
        bars = client.get_crypto_bars(request).df
    else:
        client = StockHistoricalDataClient(api_key, secret_key)
        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf_map[tf],
            start=datetime.fromisoformat(start), end=datetime.fromisoformat(end), feed='sip'
        )
        bars = client.get_stock_bars(request).df

    if bars.empty: raise ValueError(f"No Alpaca data for {symbol}")
    if isinstance(bars.index, pd.MultiIndex): bars = bars.loc[symbol]
    bars = bars.sort_index()
    opens, highs, lows, closes, volumes = bars['open'].tolist(), bars['high'].tolist(), bars['low'].tolist(), bars['close'].tolist(), bars['volume'].tolist()
    times = list(bars.index.to_pydatetime())
    return opens, highs, lows, closes, volumes, times

def fetch_unified_ohclv(symbol, user_tf, user_start, user_end, api_key=None, secret_key=None):
    try:
        return fetch_dukascopy_ohclv(symbol, user_tf, user_start, user_end)
    except Exception as e:
        print("Dukascopy:", e)
    try:
        return fetch_binance_ohclv(symbol, user_tf, user_start, user_end)
    except Exception as e:
        print("Binance:", e)
    if api_key and secret_key:
        try:
            return fetch_alpaca_ohclv(symbol, user_tf, user_start, user_end, api_key, secret_key)
        except Exception as e:
            print("Alpaca:", e)
    return None
