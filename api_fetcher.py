import requests
import time
import json
from kafka import KafkaProducer

API_KEY = "04JYE3NY6VOCUWG9"
SYMBOL = "AAPL"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_stock_data():
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": SYMBOL,
        "apikey": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "Time Series (Daily)" not in data:
        print("API Error:", data)
        return None

    return data["Time Series (Daily)"]


last_sent_date = None

while True:
    data = fetch_stock_data()

    if data:
        for day, values in data.items():

            # منع التكرار
            if day == last_sent_date:
                continue

            record = {
                "symbol": SYMBOL,
                "timestamp": day,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }

            producer.send('stocks', value=record)
            print("Sent:", record)

        # 🔥 تحديث آخر يوم بعد ما نرسل كل البيانات
        last_sent_date = list(data.keys())[0]

    print("⏳ Sleeping 6 hours...")
    time.sleep(3600 * 6)