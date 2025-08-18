import os
import requests
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import pendulum

# Load environment variables
load_dotenv()

API_KEY = os.getenv("TWELVE_DATA_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'TSLA', 'MSFT', 'NVDA']
INTERVAL = "5min"
TIMEZONE = pendulum.timezone("UTC")

def fetch_stock_data(symbol):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": INTERVAL,   
        "apikey": API_KEY,
        "outputsize": 1500,
        "format": "JSON"
    }
    response = requests.get(url, params=params)
    return response.json()

def insert_to_mongodb(symbol, data):
    if "values" not in data:
        print(f"[ERROR] No 'values' key found for {symbol}. Error: {data.get('message')}")
        return

    values = data["values"]
    if not isinstance(values, list) or not values:
        print(f"[ERROR] No valid data to insert for {symbol}")
        return

    # Add extra fields
    for record in values:
        record["symbol"] = symbol
        record["datetime"] = datetime.strptime(record["datetime"], "%Y-%m-%d %H:%M:%S")

    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client["stock_db"]
    collection = db["Ingestion_Dag"]  
    
    collection.insert_many(values)
    print(f"[SUCCESS] Inserted {len(values)} records for {symbol} into MongoDB")

def main():
    for symbol in STOCK_SYMBOLS:
        print(f"\n[INFO] Fetching data for {symbol}")
        data = fetch_stock_data(symbol)
        insert_to_mongodb(symbol, data)

if __name__ == "__main__":
    main()

