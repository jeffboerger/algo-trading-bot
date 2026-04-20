import yfinance as yf
import pandas as pd

def fetch_ohlcv(ticker: str, period: str, interval: str) -> pd.DataFrame:
    stock = yf.Ticker(ticker)
    df = stock.history(period=period, interval=interval)
    df.reset_index(inplace=True)
    return df

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df.columns = [col.lower() for col in df.columns]
    return df

if __name__ == "__main__":
    df = fetch_ohlcv("AAPL", period="3mo", interval="1d")
    df = clean_ohlcv(df)
    print(df.head(10))
    print(df.dtypes)