import time
import random
from datetime import datetime

def get_mock_tick(stock_code: str):
    return {
        "stock_code": stock_code,
        "trade_price": random.randint(70000, 73000),
        "trade_volume": random.randint(1, 500),
        "event_time": datetime.utcnow().isoformat()
    }
