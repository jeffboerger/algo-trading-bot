from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
from sklearn.utils.class_weight import compute_sample_weight

load_dotenv()

def fetch_features() -> pd.DataFrame:
    client = bigquery.Client()
    
    query = """
        select *
        from `algo-trading-bot-493914.algo_trading.fct_stock_features`
        order by ticker, date asc
    """
    
    df = client.query(query).to_dataframe()
    return df

def create_labels(df: pd.DataFrame, threshold: float = 0.01) -> pd.DataFrame:
    df = df.copy()
    
    # tomorrow's return based on today's close
    df['future_return'] = df['close'].shift(-1) / df['close'] - 1
    
    # label based on threshold
    df['label'] = 'HOLD'
    df.loc[df['future_return'] > threshold, 'label'] = 'BUY'
    df.loc[df['future_return'] < -threshold, 'label'] = 'SELL'
    
    # drop the last row — no tomorrow to predict
    df = df.dropna(subset=['future_return'])
    
    return df

def split_data(df: pd.DataFrame, train_pct: float = 0.7):
    split_idx = int(len(df) * train_pct)
    
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    
    feature_cols = ['sma_20', 'ema_12', 'ema_26', 'macd', 'rsi_14']
    
    X_train = train[feature_cols]
    y_train = train['label']
    X_test = test[feature_cols]
    y_test = test['label']
    
    return X_train, y_train, X_test, y_test

def train_model(X_train, y_train, X_test, y_test):
    # encode string labels to numbers
    le = LabelEncoder()
    y_train_enc = le.fit_transform(y_train)
    y_test_enc = le.transform(y_test)

    # compute sample weights to balance classes
    sample_weights = compute_sample_weight(class_weight='balanced', y=y_train_enc)
    
    # train XGBoost
    model = XGBClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1,
        random_state=42
    )
    
    model.fit(X_train, y_train_enc, sample_weight=sample_weights)
    
    # evaluate
    y_pred = model.predict(X_test)
    print(classification_report(y_test_enc, y_pred, target_names=le.classes_))
    
    return model, le

if __name__ == "__main__":
    df = fetch_features()
    df = create_labels(df)
    X_train, y_train, X_test, y_test = split_data(df)
    model, le = train_model(X_train, y_train, X_test, y_test)