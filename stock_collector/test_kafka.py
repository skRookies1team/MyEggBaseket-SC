from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    api_version=(3, 6, 0),
)

print("Kafka 연결 성공")
