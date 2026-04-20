from pipeline.fetch import fetch_ohlcv
from pipeline.clean import clean_ohlcv
from pipeline.load import load_to_bigquery

if __name__ == "__main__":
    df = fetch_ohlcv("AAPL", period="3mo", interval="1d")
    df = clean_ohlcv(df)
    load_to_bigquery(df, "algo-trading-bot-493914.algo_trading.aapl_daily")