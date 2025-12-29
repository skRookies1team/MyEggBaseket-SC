import json
import time
import threading
import websocket
import requests
import os
from dotenv import load_dotenv

load_dotenv()


class KISWebSocketClient:
    def __init__(self, on_message):
        self.app_key = os.getenv("KIS_APP_KEY")
        self.app_secret = os.getenv("KIS_APP_SECRET")
        self.ws_url = os.getenv("KIS_WS_URL")

        if not self.app_key or not self.app_secret:
            raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")

        self.on_message = on_message
        self.approval_key = None
        self.ws = None
        self.connected = False
        self.subscribed = set()

        self.rest_base_url = "https://openapi.koreainvestment.com:9443"

        # 처리할 TR ID
        self.TR_TICK = "H0STCNT0"
        self.TR_ORDERBOOK = "H0STASP0"

    # -------------------------------
    # Approval Key
    # -------------------------------
    def issue_approval_key(self):
        url = f"{self.rest_base_url}/oauth2/Approval"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }

        res = requests.post(url, json=payload)
        res.raise_for_status()

        self.approval_key = res.json()["approval_key"]
        print("🔑 Approval Key issued")

    # -------------------------------
    # WebSocket
    # -------------------------------
    def connect(self):
        if not self.approval_key:
            self.issue_approval_key()

        def _run():
            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_close=self._on_close,
                on_error=self._on_error,
            )
            self.ws.run_forever()

        threading.Thread(target=_run, daemon=True).start()

    def _on_open(self, ws):
        self.connected = True
        print("🔌 KIS WebSocket connected")

        for symbol in self.subscribed:
            self._send_subscribe(symbol, self.TR_TICK)
            self._send_subscribe(symbol, self.TR_ORDERBOOK)

    def _on_close(self, ws, *args):
        self.connected = False
        print("⚠️ WS closed, reconnecting...")
        time.sleep(2)
        self.connect()

    def _on_error(self, ws, error):
        print("WS error:", error)

    # -------------------------------
    # Message
    # -------------------------------
    def _on_message(self, ws, message):
        if message.startswith("{"):
            return

        parts = message.split("|")
        if len(parts) < 4:
            return

        tr_id = parts[1]
        fields = parts[3].split("^")
        symbol = fields[0]

        if symbol not in self.subscribed:
            return

        try:
            # =========================
            # 1️⃣ 실시간 체결가
            # =========================
            if tr_id == self.TR_TICK:
                tick = {
                    "type": "STOCK_TICK",
                    "stckShrnIscd": fields[0],
                    "stckCntgHour": fields[1],
                    "stckPrpr": int(float(fields[2])),
                }
                self.on_message(tick)

            # =========================
            # 2️⃣ 실시간 호가
            # =========================
            elif tr_id == self.TR_ORDERBOOK:
                asks = []
                bids = []

                # 매도호가 1~10 / 잔량
                for i in range(10):
                    asks.append({
                        "price": int(float(fields[3 + i])),
                        "qty": int(float(fields[23 + i])),
                    })

                # 매수호가 1~10 / 잔량
                for i in range(10):
                    bids.append({
                        "price": int(float(fields[13 + i])),
                        "qty": int(float(fields[33 + i])),
                    })

                order_book = {
                    "type": "ORDER_BOOK",
                    "code": symbol,
                    "time": fields[1],
                    "asks": asks,
                    "bids": bids,
                    "totalAskQty": int(float(fields[43])),
                    "totalBidQty": int(float(fields[44])),
                }

                self.on_message(order_book)

        except Exception as e:
            print("❌ 실시간 데이터 파싱 오류:", e)
            print("원본 message:", message)

    # -------------------------------
    # Subscribe
    # -------------------------------
    def subscribe(self, symbol: str):
        if symbol in self.subscribed:
            return

        self.subscribed.add(symbol)

        if self.connected:
            self._send_subscribe(symbol, self.TR_TICK)
            self._send_subscribe(symbol, self.TR_ORDERBOOK)

    def _send_subscribe(self, symbol: str, tr_id: str):
        payload = {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": symbol,
                }
            },
        }
        self.ws.send(json.dumps(payload))