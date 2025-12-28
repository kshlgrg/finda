import ccxt
import pandas as pd
from datetime import datetime

def user_to_dt(s, as_type='datetime'):
    parts = [int(p) for p in s.split('-')]
    while len(parts) < 6:
        parts.append(0)
    dt = datetime(*parts)
    return dt if as_type == 'datetime' else dt.strftime("%Y-%m-%dT%H:%M:%S")

def fetch_dukascopy_ticks(symbol, user_tf, user_start, user_end):
    from dukascopy_python import fetch, INTERVAL_TICK, OFFER_SIDE_BID
    symbol = symbol.strip().upper()
    start = user_to_dt(user_start, 'datetime')
    end = user_to_dt(user_end, 'datetime')
    df = fetch(symbol, INTERVAL_TICK, OFFER_SIDE_BID, start=start, end=end)
    if df is None or df.empty:
        raise ValueError(f"No Dukascopy tick data for {symbol}")
    bid = df["bidPrice"].tolist() if "bidPrice" in df else [0] * len(df)
    ask = df["askPrice"].tolist() if "askPrice" in df else [0] * len(df)
    bid_vol = df["bidVolume"].tolist() if "bidVolume" in df else [0] * len(df)
    ask_vol = df["askVolume"].tolist() if "askVolume" in df else [0] * len(df)
    real_vol = [0] * len(df)
    times = list(df.index.to_pydatetime())
    return bid, ask, bid_vol, ask_vol, real_vol, times

def fetch_binance_ticks(symbol, user_tf, user_start, user_end):
    exchange = ccxt.binance({'enableRateLimit': True})
    symbol = symbol.strip().upper()
    binance_symbol = symbol.replace("/", "")
    since_str = user_to_dt(user_start, 'iso')
    end_str = user_to_dt(user_end, 'iso')
    since = int(datetime.fromisoformat(since_str).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(end_str).timestamp() * 1000)
    trades = []
    while since < end_ms:
        batch = exchange.fetch_trades(binance_symbol, since=since, limit=1000)
        if not batch: break
        for t in batch:
            if t['timestamp'] > end_ms: break
            trades.append(t)
        if len(batch) < 1000: break
        since = batch[-1]['timestamp'] + 1
    if not trades:
        raise ValueError(f"No Binance tick data for {symbol}")
    bid, ask, bid_vol, ask_vol, real_vol, times = [], [], [], [], [], []
    for t in trades:
        side = t.get('side')
        if side == 'buy':
            bid.append(None)
            ask.append(t['price'])
            bid_vol.append(0)
            ask_vol.append(t['amount'])
        elif side == 'sell':
            bid.append(t['price'])
            ask.append(None)
            bid_vol.append(t['amount'])
            ask_vol.append(0)
        else:
            bid.append(None)
            ask.append(None)
            bid_vol.append(0)
            ask_vol.append(0)
        real_vol.append(t['amount'])
        times.append(datetime.fromtimestamp(t['timestamp'] / 1000))
    return bid, ask, bid_vol, ask_vol, real_vol, times

def fetch_alpaca_ticks(symbol, user_tf, user_start, user_end, api_key, secret_key):
    from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
    from alpaca.data.requests import StockTradesRequest, CryptoTradesRequest
    symbol = symbol.strip().upper()
    start = user_to_dt(user_start, 'iso')
    end = user_to_dt(user_end, 'iso')
    
    is_crypto = '/' in symbol or symbol in ['BTCUSD', 'ETHUSD']
    
    if is_crypto:
        client = CryptoHistoricalDataClient(api_key, secret_key)
        request = CryptoTradesRequest(
            symbol_or_symbols=symbol,
            start=datetime.fromisoformat(start),
            end=datetime.fromisoformat(end),
        )
        trades = client.get_crypto_trades(request).df
    else:
        client = StockHistoricalDataClient(api_key, secret_key)
        request = StockTradesRequest(
            symbol_or_symbols=symbol,
            start=datetime.fromisoformat(start),
            end=datetime.fromisoformat(end),
        )
        trades = client.get_stock_trades(request).df

    if trades.empty:
        raise ValueError(f"No Alpaca tick (trade) data for {symbol}")
    if isinstance(trades.index, pd.MultiIndex):
        trades = trades.loc[symbol]
    bid = trades['price'].tolist() if 'price' in trades else [0]*len(trades)
    ask = trades['price'].tolist() if 'price' in trades else [0]*len(trades)
    bid_vol = [0]*len(trades)
    ask_vol = [0]*len(trades)
    real_vol = trades['size'].tolist() if 'size' in trades else [0]*len(trades)
    times = list(trades.index.to_pydatetime())
    return bid, ask, bid_vol, ask_vol, real_vol, times

def fetch_unified_tick(symbol, user_tf, user_start, user_end, api_key=None, secret_key=None):
    # Try Dukascopy first
    try:
        return "dukascopy", fetch_dukascopy_ticks(symbol, user_tf, user_start, user_end)
    except Exception as e:
        print("Dukascopy:", e)
    # Try Binance next
    try:
        return "binance", fetch_binance_ticks(symbol, user_tf, user_start, user_end)
    except Exception as e:
        print("Binance:", e)
    # Try Alpaca if keys present
    if api_key and secret_key:
        try:
            return "alpaca", fetch_alpaca_ticks(symbol, user_tf, user_start, user_end, api_key, secret_key)
        except Exception as e:
            print("Alpaca:", e)
    return None, None


