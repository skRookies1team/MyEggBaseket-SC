import threading
import time

from kafka_consumer import consume_subscription_events
from kis_ws.kis_client import KISWebSocketClient
from kis_ws.kis_auth import get_approval_key


def start_kafka_consumer():
    """
    Kafka stock-subscription 토픽을 소비해서
    subscribe / unsubscribe 이벤트를 처리
    """
    consume_subscription_events()


def main():
    print(" StockCollector started")

    # 1️⃣ approval_key 자동 발급
    approval_key = get_approval_key()
    print(" approval_key 발급 완료")

    # 2️⃣ KIS WebSocket Client 생성 및 연결
    kis_client = KISWebSocketClient(approval_key)
    kis_client.connect()

    # 3️⃣ Kafka Consumer 스레드 시작
    consumer_thread = threading.Thread(
        target=start_kafka_consumer,
        daemon=True,
    )
    consumer_thread.start()

    # 4️⃣ 메인 스레드는 생존만 유지
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" StockCollector stopped")


if __name__ == "__main__":
    main()
