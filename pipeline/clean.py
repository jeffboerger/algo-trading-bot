import pandas as pd

def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df.columns = [col.lower() for col in df.columns]
    return df