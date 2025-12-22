import json
import threading
import time
import websocket
from typing import Callable, Optional


class KISWebSocketClient:
    WS_URL = "wss://openapi.koreainvestment.com:9443/websocket"

    def __init__(
        self,
        approval_key: str,
        on_tick: Optional[Callable[[dict], None]] = None,
    ):
        """
        approval_key : KIS approval_key
        on_tick      : 체결 데이터 수신 시 호출할 콜백 (Kafka 전송용)
        """
        self.approval_key = approval_key
        self.on_tick = on_tick

        self.ws = None
        self.connected = False
        self.subscribed = set()
        self.lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def connect(self):
        if self.connected:
            return
        
        headers = {
                "User-Agent": "Mozilla/5.0",
                "Origin": "https://openapi.koreainvestment.com",
            }
        
        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            header=[f"{k}: {v}" for k, v in headers.items()],
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        threading.Thread(
            target=self.ws.run_forever,
            daemon=True,
            kwargs={"ping_interval": 30, "ping_timeout": 10},
        ).start()

        # 연결 대기
        for _ in range(10):
            if self.connected:
                return
            time.sleep(0.5)

        raise RuntimeError("❌ KIS WebSocket 연결 실패")

    # ------------------------------------------------------------------
    # Subscribe / Unsubscribe
    # ------------------------------------------------------------------
    def subscribe(self, stock_code: str):
        with self.lock:
            if stock_code in self.subscribed:
                return

            msg = self._make_subscribe_msg(stock_code, tr_type="1")
            self.ws.send(json.dumps(msg))

            self.subscribed.add(stock_code)
            print(f"[KIS] SUBSCRIBE {stock_code}")

    def unsubscribe(self, stock_code: str):
        with self.lock:
            if stock_code not in self.subscribed:
                return

            msg = self._make_subscribe_msg(stock_code, tr_type="2")
            self.ws.send(json.dumps(msg))

            self.subscribed.remove(stock_code)
            print(f"[KIS] UNSUBSCRIBE {stock_code}")

    def _make_subscribe_msg(self, stock_code: str, tr_type: str):
        return {
            "header": {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": tr_type,   # 1=subscribe, 2=unsubscribe
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": stock_code,
                }
            },
        }

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------
    def _on_open(self, ws):
        self.connected = True
        print("[KIS] WebSocket connected")

        # 🔁 재연결 시 기존 구독 복구
        for code in list(self.subscribed):
            self.subscribe(code)

    def _on_message(self, ws, message: str):
        """
        메시지 종류:
        1) 체결 데이터
        2) 제어 메시지 (PING, SUBSCRIBE OK 등)
        """
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            print("[KIS] NON-JSON:", message)
            return

        # 제어 메시지
        if "header" in data and "tr_id" not in data.get("body", {}).get("output", {}):
            self._handle_control_message(data)
            return

        # 체결 데이터
        tick = self._parse_tick(data)
        if tick and self.on_tick:
            self.on_tick(tick)

    def _handle_control_message(self, data: dict):
        msg = data.get("body", {}).get("msg", "")
        if msg:
            print(f"[KIS] INFO: {msg}")

    def _parse_tick(self, data: dict) -> Optional[dict]:
        """
        H0STCNT0 체결 데이터 파싱 (최소 필드)
        """
        try:
            output = data["body"]["output"]
            return {
                "stockCode": output["stck_shrn_iscd"],
                "price": int(output["stck_prpr"]),
                "volume": int(output["cntg_vol"]),
                "change": int(output["prdy_vrss"]),
                "changeRate": float(output["prdy_ctrt"]),
                "timestamp": output["cntg_hour"],
            }
        except Exception as e:
            print("[KIS] PARSE ERROR:", e, data)
            return None

    def _on_error(self, ws, error):
        print("[KIS] ERROR:", error)

    def _on_close(self, ws, *args):
        self.connected = False
        print("[KIS] WebSocket closed")
