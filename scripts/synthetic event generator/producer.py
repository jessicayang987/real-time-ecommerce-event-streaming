from confluent_kafka import Producer
from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import json
import random
import time
import uuid


load_dotenv()

EH_NAMESPACE = os.getenv("EVENTHUB_NAMESPACE")
EH_NAME = os.getenv("EVENTHUB_NAME")
CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")

# must validate the EVENTHUB variables first after loading
# otherwise, Pylance will throw an error for not recognizing EH_NAME as a real string on line 70
if not EH_NAMESPACE:
    raise ValueError("Missing EVENTHUB_NAMESPACE in .env")

if not EH_NAME:
    raise ValueError("Missing EVENTHUB_NAME in .env")

if not CONN_STR:
    raise ValueError("Missing EVENTHUB_CONNECTION_STRING in .env")

conf = {
    "bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": CONN_STR,
    "client.id": "order-simulator",
}
producer = Producer(conf)

CATALOG = [
    ("PROD-001", "electronics", 299.99),
    ("PROD-002", "electronics", 1299.00),
    ("PROD-003", "clothing", 49.99),
    ("PROD-004", "clothing", 89.50),
    ("PROD-005", "books", 19.99),
    ("PROD-006", "home", 159.00),
    ("PROD-007", "sports", 75.00),
]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "JP", "BR", "CA"]


delivered_count = 0
failed_count = 0
def delivered(err, msg):
    global delivered_count, failed_count
    if err: 
        failed_count += 1
        print(f"ERROR: {err}")
    else:
        delivered_count += 1

def make_event():
    pid, cat, price = random.choice(CATALOG)
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "product_id": pid,
        "product_category": cat,
        "price": price,
        "quantity": random.randint(1, 5),
        "country": random.choice(COUNTRIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

print("Producing... Ctrl+C to stop.")
n = 0

# try/except KeyboardInterrupt/finally structure for clean shutdown
try:
    while True:
        producer.produce(EH_NAME, json.dumps(make_event()).encode(), callback=delivered)
        producer.poll(0)
        n += 1
        if n % 1000 == 0: 
            producer.flush()  # add periodic flush to avoid in-flight msg accumulation in local buffer
            print(f"sent {n}")
        time.sleep(0.01)      # ~100 events/sec
except KeyboardInterrupt:
    print("\nStopping. Flushing remaining messages...")
finally:
    producer.flush(timeout=10) #  Final flush(timeout=10) in finally so unflushed events get drained on Ctrl+C
    print(f"Done. Submitted: {n} | Confirmed: {delivered_count} | Failed: {failed_count}")
    # Final flush() is used to safely finish sending remaining buffered messages to Event Hubs and print a final summary.