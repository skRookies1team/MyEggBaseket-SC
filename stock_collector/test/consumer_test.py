from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "stock-subscription",
    bootstrap_servers="localhost:9092",
    group_id="stock-collector-test",
    auto_offset_reset="earliest",  # 테스트용
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("구독 이벤트 대기 중...")

for msg in consumer:
    print("수신 이벤트:", msg.value)
