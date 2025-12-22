from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

producer.send(
    "stock-subscription",
    {
        "action": "subscribe",
        "stockCode": "005930",
    }
)

producer.flush()
print("이벤트 전송 완료")
