from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

def send_stock_tick(stock_code: str, tick: dict):
    producer.send(
        topic="stock-ticks",
        key=stock_code,
        value=tick
    )
    producer.flush()
pip 