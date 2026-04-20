import yfinance as yf
import pandas as pd

def fetch_ohlcv(ticker: str, period: str, interval: str) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    df = stock.history(period=period, interval=interval)
    df.reset_index(inplace=True)
    return df

if __name__ == "__main__":
    df = fetch_ohlcv("AAPL", period="3mo", interval="1d")
    print(df.head(10))
    print(df.dtypes)