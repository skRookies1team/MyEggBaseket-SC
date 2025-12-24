from kafka_client import create_consumer, create_producer
from subscription_manager import SubscriptionManager
from kis_ws_client import KISWebSocketClient
from config import *
import json


def main():
    consumer = create_consumer(
        topic=TOPIC_SUBSCRIBE,
        group_id=GROUP_ID,
        servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    sub_manager = SubscriptionManager()

    def on_tick(tick):
        # 주식 데이터가 들어오면 Kafka로 재전송 & 로그 출력
        producer.produce(
            TOPIC_PUBLISH,
            value=json.dumps(tick).encode("utf-8"),
        )
        producer.flush()
        print(f"tick: {tick.get('stckShrnIscd')} ({tick.get('stckPrpr')})")

    kis_ws = KISWebSocketClient(on_tick=on_tick)
    kis_ws.connect()

    print("StockCollector with KIS WS started")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            # 1. 메시지 디코딩
            data_str = msg.value().decode("utf-8")
            event = json.loads(data_str)

            # 2. 필드 매핑 (제공해주신 형식에 맞춤)
            # eventType이 있으면 그걸 쓰고, 없으면 action을 찾음
            action = event.get("eventType") or event.get("action")

            # stockCode가 있으면 그걸 쓰고, 없으면 symbol을 찾음
            symbol = event.get("stockCode") or event.get("symbol") or event.get("code")

            # 3. 데이터 검증
            if not action or not symbol:
                print(f"⚠️ [Skip] 필수 데이터 누락: {event}")
                continue

            # 4. 구독/해제 실행 (대소문자 무시)
            action_upper = str(action).upper()

            if action_upper == "SUBSCRIBE":
                # 중복 구독 방지 (SubscriptionManager가 관리)
                is_first = sub_manager.subscribe(symbol)
                if is_first:
                    kis_ws.subscribe(symbol)
                    print(f"✅ [구독] {symbol} (요청: {action})")
                else:
                    print(f"ℹ️ [중복] {symbol} 이미 구독 중")

            elif action_upper == "UNSUBSCRIBE":
                is_last = sub_manager.unsubscribe(symbol)
                if is_last:
                    kis_ws.unsubscribe(symbol)
                    print(f"👋 [해제] {symbol} (요청: {action})")

            else:
                print(f"⚠️ [Skip] 알 수 없는 이벤트: {action}")

        except json.JSONDecodeError:
            print(f"⚠️ [Skip] JSON 파싱 실패: {msg.value()}")
        except Exception as e:
            print(f"⚠️ [Error] 처리 중 오류: {e}")


if __name__ == "__main__":
    main()