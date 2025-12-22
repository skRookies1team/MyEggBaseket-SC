from kafka import KafkaConsumer
import json
from kis_ws.kis_client import KISWebSocketClient

APPROVAL_KEY = "YOUR_KIS_APPROVAL_KEY"

kis_client = KISWebSocketClient(APPROVAL_KEY)
kis_client.connect()

consumer = KafkaConsumer(
    "stock-subscription",
    bootstrap_servers="localhost:9092",
    group_id="stock-collector",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Kafka 이벤트 수신 대기...")

for msg in consumer:
    event = msg.value
    action = event.get("action")
    code = event.get("stockCode")

    if action == "subscribe":
        kis_client.subscribe(code)

    elif action == "unsubscribe":
        kis_client.unsubscribe(code)
