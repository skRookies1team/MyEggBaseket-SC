import json
import time
import threading
import websocket
import requests
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()


class KisWSClient:
    def __init__(self, app_key, app_secret, approval_key=None, mode="REAL"):
        """
        main.py의 호출 방식에 맞춘 초기화 메서드
        """
        self.app_key = app_key
        self.app_secret = app_secret
        self.approval_key = approval_key
        self.mode = mode

        # 모드에 따른 URL 설정
        if self.mode == "REAL":
            self.rest_base_url = "https://openapi.koreainvestment.com:9443"
            self.ws_url = "ws://ops.koreainvestment.com:21000"
        else:
            self.rest_base_url = "https://openapivts.koreainvestment.com:29443"
            self.ws_url = "ws://ops.koreainvestment.com:21000/tryitout/websocket"

        self.ws = None
        self.connected = False
        self.subscribed: set[str] = set()

        # 틱 데이터 처리 콜백 (기본값: 콘솔 출력)
        self.on_tick = lambda t: print(f"[TICK] {t['stckShrnIscd']} : {t['stckPrpr']}")

    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        headers = {"content-type": "application/json"}
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }

        try:
            res = requests.post(url, headers=headers, data=json.dumps(payload))
            if res.status_code != 200:
                print(f"Approval Key 발급 실패 (Status {res.status_code}): {res.text}")
                return

            self.approval_key = res.json()["approval_key"]
            print(f"🔑 Approval Key 발급 완료 ({self.mode})")
        except Exception as e:
            print(f"Approval Key 요청 중 오류 발생: {e}")

    # ------------------------------------------------------------------
    # Async Wrapper Methods (main.py 호환용)
    # ------------------------------------------------------------------
    async def connect(self):
        if not self.approval_key:
            self.issue_approval_key()

        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self.ws.run_forever()

        # 웹소켓 스레드 시작
        threading.Thread(target=_run, daemon=True).start()

        # 연결될 때까지 대기
        print("Connecting to KIS WebSocket...")
        for _ in range(50):  # 최대 5초 대기
            if self.connected:
                print("✅ Connected!")
                return
            await asyncio.sleep(0.1)
        print("⚠️ WebSocket connection might be delayed.")

    async def subscribe_list(self, symbol_list):
        """리스트 형태의 종목들을 한 번에 구독"""
        for symbol in symbol_list:
            self.subscribe(symbol)
            # API 부하 방지를 위해 미세한 딜레이 (선택사항)
            # await asyncio.sleep(0.01)

    async def close(self):
        if self.ws:
            self.ws.close()
        self.connected = False

    # ------------------------------------------------------------------
    # WebSocket Event Handlers
    # ------------------------------------------------------------------
    def _on_open(self, ws):
        self.connected = True
        print(f"🔌 KIS WebSocket connected ({self.mode})")
        # 재연결 시 기존 구독 복구
        for symbol in self.subscribed:
            self._send_subscribe(symbol)

    def _on_close(self, ws, *args):
        self.connected = False
        print("KIS WebSocket closed")
        # 자동 재연결 로직은 connect() 호출자가 관리하거나 여기서 처리

    def _on_error(self, ws, error):
        print("KIS WebSocket error:", error)

    def _on_message(self, ws, message):
        if message.startswith("{"):  # 핑퐁 메시지 등 무시
            return

        parts = message.split("|")
        if len(parts) < 4:
            return

        # H0STCNT0: 실시간 주식 체결가
        if parts[1] != "H0STCNT0":
            return

        f = parts[3].split("^")
        symbol = f[0]

        try:
            tick = {
                "stckShrnIscd": f[0],  # 종목코드
                "stckCntgHour": f[1],  # 체결시간
                "stckPrpr": self.to_int(f[2]),  # 현재가
                "prdyVrss": self.to_float(f[4]),  # 전일대비
                "prdyCtrt": self.to_float(f[5]),  # 등락률
                "acmlVol": self.to_int(f[9]),  # 누적거래량
                "acmlTrPbmn": self.to_int(f[10]),  # 누적거래대금
                "askp1": self.to_int(f[13]),  # 매도호가1
                "bidp1": self.to_int(f[14]),  # 매수호가1
            }

            # 콜백 호출
            if self.on_tick:
                self.on_tick(tick)

        except Exception as e:
            # print("체결 데이터 파싱 오류:", e)
            pass

    # ------------------------------------------------------------------
    # 구독 로직
    # ------------------------------------------------------------------
    def subscribe(self, symbol):
        if not symbol: return
        if symbol in self.subscribed: return

        self.subscribed.add(symbol)
        if self.connected:
            self._send_subscribe(symbol)
            print(f"📡 Subscribed: {symbol}")

    def unsubscribe(self, symbol):
        if symbol not in self.subscribed: return
        self.subscribed.remove(symbol)
        # KIS 웹소켓은 명시적 구독 취소 API가 제한적이므로 내부 관리만 수행
        print(f"Unsubscribed: {symbol}")

    def _send_subscribe(self, symbol):
        if not self.approval_key:
            return

        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": symbol,
                }
            },
        }
        try:
            self.ws.send(json.dumps(payload))
        except Exception as e:
            print(f"Send Error: {e}")

    @staticmethod
    def to_int(v):
        try:
            return int(float(v))
        except:
            return 0

    @staticmethod
    def to_float(v):
        try:
            return float(v)
        except:
            return 0.0