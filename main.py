import time
from api_fetcher import fetch_stock_data
from kafka_producer import send_to_kafka

data = fetch_stock_data()

while True:
    for day, values in data["Time Series (Daily)"].items():

        record = {
            "date": day,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        }

        send_to_kafka(record)
        time.sleep(2)

    time.sleep(60)