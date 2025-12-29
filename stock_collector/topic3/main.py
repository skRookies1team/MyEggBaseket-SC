import os
import asyncio
import json
from datetime import datetime
from kis_ws_client import KisWSClient
from kafka_client import KafkaConsumerClient, KafkaProducerClient  # KafkaProducerClient 필요
from subscription_manager import SubscriptionManager

# Load .env
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
APP_KEY_2 = os.getenv("APP_KEY_2")
APP_SECRET_2 = os.getenv("APP_SECRET_2")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

_missing = [name for name, val in (
    ("APP_KEY", APP_KEY),
    ("APP_SECRET", APP_SECRET),
    ("APP_KEY_2", APP_KEY_2),
    ("APP_SECRET_2", APP_SECRET_2),
    ("KAFKA_BROKER", KAFKA_BROKER),
) if not val]

if _missing:
    print(f"Warning: Missing env vars: {', '.join(_missing)}")


# -----------------------------------------------------------------------------
# 틱 데이터 처리 핸들러 (KIS WS -> Kafka Producer)
# -----------------------------------------------------------------------------
def handle_tick(tick_data, producer):
    """
    KIS 웹소켓에서 수신한 틱 데이터를 백엔드 DTO 포맷으로 변환하여 Kafka로 전송
    """
    try:
        # 시간 파싱 (HHMMSS -> ISO Format)
        now = datetime.now()
        time_str = tick_data.get('stckCntgHour', now.strftime('%H%M%S'))

        # 시간 포맷이 올바른지 확인 후 적용
        if len(time_str) == 6 and time_str.isdigit():
            dt = now.replace(
                hour=int(time_str[:2]),
                minute=int(time_str[2:4]),
                second=int(time_str[4:6]),
                microsecond=0
            )
        else:
            dt = now

        # 백엔드 StockTickDTO 구조에 맞춤
        payload = {
            "stockCode": tick_data.get('stckShrnIscd'),
            "currentPrice": tick_data.get('stckPrpr'),
            "timestamp": dt.isoformat(),
            "changeRate": tick_data.get('prdyCtrt', 0.0),
            "volume": tick_data.get('acmlVol', 0)
        }

        # Kafka 전송 (토픽명: stock-ticks)
        producer.send("stock-ticks", payload)
        # print(f"[Tick] Sent {payload['stockCode']} : {payload['currentPrice']}")

    except Exception as e:
        print(f"Error sending tick to Kafka: {e}")


# -----------------------------------------------------------------------------
# Main Loop
# -----------------------------------------------------------------------------
async def main():
    sub_manager = SubscriptionManager()

    # [중요] Kafka Producer 생성 (체결가 전송용)
    print(f"Connecting to Kafka Broker: {KAFKA_BROKER}")
    kafka_producer = KafkaProducerClient(broker=KAFKA_BROKER)

    # 콜백 함수 정의 (closure로 producer 주입)
    on_tick_callback = lambda t: handle_tick(t, kafka_producer)

    # 1. 관리자용 클라이언트 (고정 50개)
    print("Initialize Admin Client...")
    client_admin = KisWSClient(
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        mode="REAL"
    )
    client_admin.on_tick = on_tick_callback  # 콜백 연결

    # 2. 사용자용 클라이언트 (관심 + 조회 종목)
    print("Initialize User Client...")
    client_user = KisWSClient(
        app_key=APP_KEY_2,
        app_secret=APP_SECRET_2,
        mode="REAL"
    )
    client_user.on_tick = on_tick_callback  # 콜백 연결

    # Kafka 소비자 (백엔드 구독 이벤트 수신용)
    # 토픽명을 백엔드 Producer 설정(subscription-events)과 일치시켜야 함
    kafka_consumer = KafkaConsumerClient(
        broker=KAFKA_BROKER,
        topic="subscription-events",
        group_id="sc-group-v1"
    )

    # KIS 웹소켓 연결
    await client_admin.connect()
    await client_user.connect()

    # 초기 구독: 관리자 계정 고정 리스트
    fixed_list = sub_manager.get_fixed_list()
    if fixed_list:
        print(f"Subscribing fixed list ({len(fixed_list)})...")
        await client_admin.subscribe_list(fixed_list)

    print("Stock Collector Started with Dual Accounts & Kafka Pipeline...")

    try:
        while True:
            # Kafka 메시지 폴링 (구독 요청 확인)
            messages = kafka_consumer.poll(timeout=0.1)

            for msg in messages:
                try:
                    val = msg.value().decode('utf-8')
                    data = json.loads(val)

                    # 백엔드 SubscriptionEventDTO 구조
                    # { "stockCode": "...", "eventType": "SUBSCRIBE", "subType": "VIEW" ... }
                    stock_code = data.get('stockCode')
                    event_type = data.get('eventType', 'SUBSCRIBE')
                    sub_type = data.get('subType', 'VIEW')

                    if not stock_code:
                        continue

                    if event_type == 'UNSUBSCRIBE':
                        # 해지 로직 (필요 시 구현, 여기선 생략하거나 로깅만)
                        # print(f"Unsubscribe request: {stock_code}")
                        continue

                    needs_update = False

                    if sub_type == 'VIEW':
                        # 조회용 리스트(Dynamic Queue)에 추가
                        if sub_manager.add_viewing_stock(stock_code):
                            print(f"[SC] Added VIEW stock: {stock_code}")
                            needs_update = True

                    elif sub_type == 'INTEREST':
                        # 관심 종목 리스트에 추가
                        sub_manager.interest_stocks.add(stock_code)
                        print(f"[SC] Added INTEREST stock: {stock_code}")
                        needs_update = sub_manager._refresh_user_account_list()

                    # 사용자 계정 구독 리스트 갱신
                    if needs_update:
                        current_list = sub_manager.get_user_dynamic_list()
                        print(f"Refreshing User Subscriptions ({len(current_list)} stocks)")
                        await client_user.subscribe_list(current_list)

                except Exception as e:
                    print(f"Message Processing Error: {e}")

            await asyncio.sleep(0.1)

    except KeyboardInterrupt:
        print("Shutting down...")
        await client_admin.close()
        await client_user.close()
        kafka_consumer.close()
        # kafka_producer는 별도 close 메서드가 없다면 생략 가능


if __name__ == "__main__":
    asyncio.run(main())