from kafka import KafkaConsumer
import json
import psycopg2

# Kafka Consumer
consumer = KafkaConsumer(
    'stocks',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PostgreSQL Connection
conn = psycopg2.connect(
    host="localhost",
    database="stocks_db",
    user="postgres",
    password="postgres",
    port=5432
)

cursor = conn.cursor()

print("🚀 Consumer + DB Running...")

for message in consumer:
    data = message.value
    print("Received:", data)

    try:
        cursor.execute("""
            INSERT INTO stock_prices 
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data["symbol"],
            data["timestamp"],
            data["open"],
            data["high"],
            data["low"],
            data["close"],
            data["volume"]
        ))

        conn.commit()
        print("✅ Inserted into DB")

    except Exception as e:
        print("❌ DB Error:", e)
        conn.rollback()