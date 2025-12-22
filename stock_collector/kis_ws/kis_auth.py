import os
import requests
from dotenv import load_dotenv

load_dotenv()

KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_SECRET_KEY = os.getenv("KIS_APP_SECRET")

if not KIS_APP_KEY or not KIS_SECRET_KEY:
    raise RuntimeError("❌ KIS_APP_KEY / KIS_APP_SECRET 누락")

# 실전 URL만 사용
APPROVAL_URL = "https://openapi.koreainvestment.com:9443/oauth2/Approval"


def get_approval_key() -> str:
    """
    실시간(WebSocket) 접속키 발급 (실전)
    """
    payload = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "secretkey": KIS_SECRET_KEY,
    }

    headers = {
        "content-type": "application/json; utf-8"
    }

    res = requests.post(
        APPROVAL_URL,
        json=payload,
        headers=headers,
        timeout=5,
    )

    res.raise_for_status()

    data = res.json()
    approval_key = data.get("approval_key")

    if not approval_key:
        raise RuntimeError(f"❌ approval_key 발급 실패: {data}")

    return approval_key
