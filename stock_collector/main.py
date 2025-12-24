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

            # 2. 유연한 필드 파싱 (여러 가능성 체크)
            # action 키 찾기: action -> eventType -> type 순서
            action = event.get("action") or event.get("eventType") or event.get("type")

            # symbol 키 찾기: symbol -> code -> stockCode -> stock_code 순서
            symbol = event.get("symbol") or event.get("code") or event.get("stockCode") or event.get("stock_code")

            # 3. 필수 정보가 없으면 전체 내용 출력 후 건너뛰기
            if not action or not symbol:
                print(f"⚠️ [Skip] 알 수 없는 포맷 (내용 확인 필요): {json.dumps(event, ensure_ascii=False)}")
                continue

            # 4. 구독/해제 로직 실행
            # 대소문자 구분 없이 처리 (Subscribe, SUBSCRIBE 등)
            action_upper = str(action).upper()

            if action_upper == "SUBSCRIBE":
                first = sub_manager.subscribe(symbol)
                if first:
                    kis_ws.subscribe(symbol)
                    print(f"✅ [구독] {symbol} (이벤트: {action})")

            elif action_upper == "UNSUBSCRIBE":
                last = sub_manager.unsubscribe(symbol)
                if last:
                    kis_ws.unsubscribe(symbol)
                    print(f"👋 [해제] {symbol} (이벤트: {action})")

            else:
                print(f"⚠️ [Skip] 알 수 없는 명령: {action} (전체: {event})")

        except json.JSONDecodeError:
            print(f"⚠️ [Skip] JSON 파싱 실패: {msg.value()}")
        except Exception as e:
            print(f"⚠️ [Error] 처리 중 오류: {e}")


if __name__ == "__main__":
    main()