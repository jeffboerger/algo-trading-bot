from pipeline.fetch import fetch_ohlcv
from pipeline.clean import clean_ohlcv
from pipeline.load import load_to_bigquery

TICKERS = ["AAPL", "DIS", "TSLA"]

if __name__ == "__main__":
    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        df = fetch_ohlcv(ticker, period="2y", interval="1d")
        df = clean_ohlcv(df)
        table_id = f"algo-trading-bot-493914.algo_trading.{ticker.lower()}_daily"
        load_to_bigquery(df, table_id)
        print(f"Loaded {ticker} to BigQuery")