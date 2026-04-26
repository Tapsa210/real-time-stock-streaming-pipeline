import time
import json
import random
from kafka import KafkaProducer

# =========================
# Kafka Setup
# =========================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# =========================
# Initial Stock Price
# =========================
price = 270.0

symbol = "AAPL"

print("🚀 Real-Time Simulator Started...")

# =========================
# Streaming Loop
# =========================
while True:
    # simulate market movement
    change = random.uniform(-1.5, 1.5)
    price += change

    record = {
        "symbol": symbol,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "open": round(price + random.uniform(-0.5, 0.5), 2),
        "high": round(price + random.uniform(0, 2), 2),
        "low": round(price - random.uniform(0, 2), 2),
        "close": round(price, 2),
        "volume": random.randint(1000000, 50000000)
    }

    producer.send('stocks', value=record)
    print("Sent:", record)

    time.sleep(2)  # 🔥 real-time every 2 seconds