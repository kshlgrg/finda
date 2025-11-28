# Finda 🔍

**Finda** is a powerful, unified financial data fetching engine designed to simplify access to historical market data. It seamlessly integrates multiple providers—**Dukascopy**, **Binance**, and **Alpaca**—into a single, robust interface for retrieving **OHLCV** (Open, High, Low, Close, Volume) and **Tick** data.

Whether you are building a backtesting engine, a trading bot, or a market analysis tool, Finda ensures you get the data you need by automatically falling back to alternative providers if your primary source is unavailable.

## 🚀 Key Features

*   **Unified API**: One function call to rule them all. Forget about managing different client libraries for stocks, crypto, and forex.
*   **Smart Fallback System**: Automatically retries with different providers (Dukascopy → Binance → Alpaca) to ensure high data availability.
*   **Multi-Asset Support**:
    *   **Forex**: High-quality data via Dukascopy.
    *   **Crypto**: Extensive coverage via Binance and Alpaca.
    *   **Stocks**: US Equity data via Alpaca.
*   **Flexible Timeframes**: Supports standard formats like `1m`, `1h`, `1d`, as well as provider-specific formats.
*   **FastAPI Service**: Comes with a built-in REST API for easy integration into microservices architectures.

## 🛠️ Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/finda.git
    cd finda
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up Environment Variables:**
    Create a `.env` file in the root directory to configure your API keys (optional, but recommended for Alpaca):
    ```env
    ALPACA_API_KEY=your_api_key
    ALPACA_SECRET_KEY=your_secret_key
    ```

## 📖 Usage

### Python Library

You can use `finda` directly in your Python scripts:

```python
from finda.ohlcv_fetcher import fetch_unified_ohclv
from finda.tick_fetcher import fetch_unified_tick

# Fetch OHLCV Data (Auto-fallback)
# Returns: (opens, highs, lows, closes, volumes, times)
data = fetch_unified_ohclv(
    symbol="BTC/USDT",
    user_tf="1h",
    user_start="2023-01-01-00-00-00",
    user_end="2023-01-02-00-00-00",
    api_key="your_alpaca_key",
    secret_key="your_alpaca_secret"
)

# Fetch Tick Data
# Returns: (provider_name, (bid, ask, bid_vol, ask_vol, real_vol, times))
provider, ticks = fetch_unified_tick(
    symbol="EUR/USD",
    user_tf="tick",
    user_start="2023-01-01-12-00-00",
    user_end="2023-01-01-13-00-00"
)
```

### REST API

Start the server:
```bash
uvicorn main:app --reload
```

**Endpoints:**

*   `GET /ohlcv`: Fetch OHLCV candles.
    *   `symbol`: e.g., `BTC/USDT`
    *   `tf`: e.g., `1h`
    *   `start`: `YYYY-MM-DD-HH-MM-SS`
    *   `end`: `YYYY-MM-DD-HH-MM-SS`
*   `GET /tick`: Fetch tick data.

## 🧪 Testing

Finda comes with a comprehensive test suite to ensure reliability.

```bash
# Run deep unified tests
python tests/unified_deep_test.py
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License
