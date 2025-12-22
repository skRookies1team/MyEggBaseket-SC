import websocket
import json
import requests
import sys
import os

# ⚠️ 재발급 받은 App Key와 Secret을 여기에 입력하세요
APP_KEY = "PSGE9bgjsCf5GBGWoe2dSP9ccuKdvtSIEs7k"
APP_SECRET = "y6rdgx/plHm4ENnHprsWWnvfpTEXHpAYYMVE7bFRFXqcCIPc9qSuekbz73mn0GLGuSnnPcacRgqhW/B2Uad4/mG1MYKivkmGFvKaT5GXBi7KQWvQh4DvQBvKwmYEZCjfpHrdzDyX6MJTFzj4DxGnE2Z5Fgnw5emDabskEZzLvTblwe/HsM8="

REST_BASE_URL = "https://openapi.koreainvestment.com:9443"

# 2. [WebSocket] 모의투자 전용 주소 (문서 기준)
# 주의: wss가 아닌 ws:// 입니다. 포트는 31000 입니다.
WS_URL = "ws://ops.koreainvestment.com:21000/tryitout/websocket"

# Approval Key 발급 함수
def get_approval_key():
    url = f"{REST_BASE_URL}/oauth2/Approval"
    headers = {"content-type": "application/json"}
    payload = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET
    }

    try:
        res = requests.post(url, headers=headers, data=json.dumps(payload))
        if res.status_code != 200:
            print(f"키 발급 실패! {res.text}")
            sys.exit(1)
        return res.json()["approval_key"]
    except Exception as e:
        print(f"오류 발생: {e}")
        sys.exit(1)

# 키 발급 실행
print(">>> Approval Key 발급 중...")
APPROVAL_KEY = get_approval_key()
print(f">>> Approval Key 발급 성공: {APPROVAL_KEY[:10]}...")

# WebSocket 이벤트 핸들러
def on_open(ws):
    print(">>> WebSocket 연결 성공! 서버에 구독 요청을 보냅니다.")

    # 실시간 체결가(H0STCNT0) 구독 요청
    # 공유해주신 문서는 시간외(H0STOAC0)였으나, 장중 테스트를 위해 체결가(H0STCNT0)로 설정함
    subscribe_data = {
        "header": {
            "approval_key": APPROVAL_KEY,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",  # 국내주식 실시간 체결가
                "tr_key": "005930"    # 삼성전자
            }
        }
    }
    ws.send(json.dumps(subscribe_data))

def on_message(ws, message):
    try:
        if message.strip().startswith('{'):
            # JSON 메시지 처리
            data = json.loads(message)
            if "body" in data and "output" in data["body"]:
                output = data["body"]["output"]
                print(f"전체 JSON 필드: {output}")
            else:
                print("실시간 데이터:", message)
        else:
            parts = message.split('|')
            if len(parts) >= 4:
                data_fields = parts[3].split('^')
                print(f"전체 필드: {data_fields}")
            else:
                print("실시간 데이터:", message)
    except Exception as e:
        print("메시지 파싱 오류:", e)
        print("원본 메시지:", message)

def on_error(ws, error):
    print(f"!!! 에러: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket 종료: {close_status_code}, {close_msg}")

if __name__ == "__main__":
    # 모의투자(ws://)는 ssl 옵션이 필요 없습니다.
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    print(f">>> 접속 시도: {WS_URL}")
    ws.run_forever()