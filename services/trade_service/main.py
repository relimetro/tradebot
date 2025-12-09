import time
import os
import numpy as np
import pandas as pd
import joblib
from datetime import datetime
from binance.client import Client
from pymongo import MongoClient
import schedule

class Binance:
    def __init__(self, public_key = '', secret_key = '', sync = False):
        self.time_offset = 0
        self.b = Client(public_key, secret_key)

        if sync:
            self.time_offset = self._get_time_offset()

    def _get_time_offset(self):
        res = self.b.get_server_time()
        return res['serverTime'] - int(time.time() * 1000)

    def synced(self, fn_name, **args):
        args['timestamp'] = int(time.time() - self.time_offset)
        return getattr(self.b, fn_name)(**args)

SYMBOL = "ETHEUR"
ASSET = "ETH"
QUOTE = "EUR"

INTERVAL = Client.KLINE_INTERVAL_1HOUR
MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "market_data"

COL_KLINES = "klines"
COL_PREDICTIONS = "predictions"
COL_TRADES = "trades"
COL_BALANCE = "balances"

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

client = Binance(public_key=API_KEY, secret_key=API_SECRET, sync = True)
mongo = MongoClient(MONGO_URI)

col_klines = mongo[DB_NAME][COL_KLINES]
col_predictions = mongo[DB_NAME][COL_PREDICTIONS]
col_trades = mongo[DB_NAME][COL_TRADES]
col_balance = mongo[DB_NAME][COL_BALANCE]

scaler = None
xgb_model = None

def add_features(df):
    df['mean_price'] = df[['Open', 'High', 'Low', 'Close']].mean(axis=1)
    df['return'] = (df['Close'] - df['Open']) / df['Open']
    df['price_change'] = df['Close'].pct_change()
    df['volatility'] = (df['High'] - df['Low']) / df['Open']
    df['body_size'] = np.abs(df['Close'] - df['Open'])
    df['upper_shadow'] = df['High'] - df[['Open', 'Close']].max(axis=1)
    df['lower_shadow'] = df[['Open', 'Close']].min(axis=1) - df['Low']
    df['candle_shape'] = (df['Close'] - df['Low']) / (df['High'] - df['Low'] + 1e-9)

    df['previous_close'] = df['Close'].shift(1)
    df['true_range'] = np.maximum.reduce([
        df['High'] - df['Low'],
        np.abs(df['High'] - df['previous_close']),
        np.abs(df['Low'] - df['previous_close'])
    ])
    df['avg_true_range_14'] = df['true_range'].rolling(14).mean()

    df['buy_ratio'] = df['Taker Buy Base Asset Volume'] / (df['Volume'] + 1e-9)
    df['normalized_volume'] = (
        (df['Volume'] - df['Volume'].rolling(24).mean()) /
        (df['Volume'].rolling(24).std() + 1e-9)
    )

    for window in [3, 6, 12, 24, 48]:
        df[f'momentum_{window}h'] = df['Close'] / df['Close'].shift(window) - 1
        df[f'rolling_return_mean_{window}'] = df['price_change'].rolling(window).mean()
        df[f'rolling_volatility_{window}'] = df['price_change'].rolling(window).std()

    for window in [6, 12, 24, 48]:
        df[f'sma_{window}'] = df['Close'].rolling(window).mean()
        df[f'ema_{window}'] = df['Close'].ewm(span=window, adjust=False).mean()
        df[f'sma_ratio_{window}'] = df['Close'] / (df[f'sma_{window}'] + 1e-9)
        df[f'ema_ratio_{window}'] = df['Close'] / (df[f'ema_{window}'] + 1e-9)

    delta = df['Close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    roll_up = pd.Series(gain).rolling(14).mean()
    roll_down = pd.Series(loss).rolling(14).mean()
    rs = roll_up / (roll_down + 1e-9)
    df['RSI_14'] = 100 - (100 / (1 + rs))

    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = df['MACD'] - df['MACD_signal']

    df['BB_middle'] = df['Close'].rolling(20).mean()
    df['BB_upper'] = df['BB_middle'] + 2 * df['Close'].rolling(20).std()
    df['BB_lower'] = df['BB_middle'] - 2 * df['Close'].rolling(20).std()
    df['BB_width'] = (df['BB_upper'] - df['BB_lower']) / df['BB_middle']

    low14 = df['Low'].rolling(14).min()
    high14 = df['High'].rolling(14).max()
    df['stoch_k'] = 100 * (df['Close'] - low14) / (high14 - low14 + 1e-9)
    df['stoch_d'] = df['stoch_k'].rolling(3).mean()

    df = df.dropna().reset_index(drop=True)

    df['target'] = df['Close'].shift(-3) / df['Close'] - 1
    df = df.dropna(subset=['target'])

    return df


def run_prediction(df_row):
    global scaler, xgb_model

    if scaler is None:
        with open("scaler.pkl", "rb") as f:
            scaler = joblib.load(f)

    if xgb_model is None:
        with open("xgb_model.pkl", "rb") as f:
            xgb_model = joblib.load(f)

    df = pd.DataFrame([df_row])
    # ignore_cols = ["Open time", "Close time", "_id", "target"]
    # feature_cols = [c for c in df.columns if c not in ignore_cols]
    feature_cols = ["High", "Open", "Close", "avg_true_range_14", "momentum_48h", "rolling_return_mean_24", "BB_width", "ema_24", "MACD", "BB_middle"]

    X = df[feature_cols].astype(float)
    X_scaled = scaler.transform(X)

    prediction = float(xgb_model.predict(X_scaled)[0])
    return prediction


def save_prediction_to_db(pred, timestamp):
    col_predictions.insert_one({
        "datetime": timestamp,
        "symbol": SYMBOL,
        "prediction": pred
    })


def get_balances():
    acc = client.synced("get_account")
    balances = {b["asset"]: float(b["free"]) for b in acc["balances"]}

    # store in DB
    col_balance.insert_one({
        "datetime": datetime.utcnow(),
        "balances": balances
    })

    return balances


def execute_trade(prediction, close_price):
    balances = get_balances()
    eur = balances.get(QUOTE, 0.0)
    eth = balances.get(ASSET, 0.0)

    trade = None

    # BUY SIGNAL
    if prediction > 0.0 and eur > 10:
        qty = round(eur / close_price, 6)
        order = client.synced("order_market_buy", symbol=SYMBOL, quantity=qty)
        trade = {
            "datetime": datetime.utcnow(),
            "type": "BUY",
            "qty": qty,
            "price": close_price,
            "prediction": prediction
        }

    # SELL SIGNAL
    elif prediction < 0.0 and eth > 0.0001:
        order = client.synced("order_market_sell", symbol=SYMBOL, quantity=eth)
        trade = {
            "datetime": datetime.utcnow(),
            "type": "SELL",
            "qty": eth,
            "price": close_price,
            "prediction": prediction
        }

    if trade:
        col_trades.insert_one(trade)
        print("TRADE EXECUTED:", trade)
    else:
        print("No trade executed.")

    get_balances()


def fetch_latest_kline():
    print("Fetching latest kline...")

    klines = client.b.get_klines(symbol=SYMBOL, interval=INTERVAL, limit=100)
    df_new = process_raw_klines(klines)

    df_old = pd.DataFrame(list(col_klines.find().sort("Open time", -1).limit(200)))
    if len(df_old) > 0:
        df_old = df_old.drop(columns=["_id"])
        df = pd.concat([df_old.iloc[::-1], df_new], ignore_index=True)
    else:
        df = df_new

    df = add_features(df)
    row = df.iloc[-1].to_dict()
    close_price = row["Close"]

    prediction = run_prediction(row)
    timestamp = datetime.fromtimestamp(row["Open time"] / 1000)

    save_prediction_to_db(prediction, timestamp)

    execute_trade(prediction, close_price)

    col_klines.update_one({"Open time": row["Open time"]}, {"$set": row}, upsert=True)

    print(f"[{timestamp}] Prediction={prediction}")


def process_raw_klines(klines):
    df = pd.DataFrame(klines, columns=[
        "Open time", "Open", "High", "Low", "Close", "Volume",
        "Close time", "Quote asset volume", "Number trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Unused"
    ])
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(float)
    df["Taker Buy Base Asset Volume"] = df["Taker Buy Base Asset Volume"].astype(float)
    return df


def initialize_database():
    if col_klines.count_documents({}) == 0:
        print("Mongo empty → downloading full history...")
        klines = client.b.get_historical_klines(SYMBOL, INTERVAL, "2017-01-01")
        df = process_raw_klines(klines)
        df = add_features(df)
        col_klines.insert_many(df.to_dict("records"))
        print("Full history inserted:", len(df))


def run_scheduler():
    schedule.every().hour.do(fetch_latest_kline)
    print("Service running...")

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    initialize_database()
    fetch_latest_kline()
    run_scheduler()

