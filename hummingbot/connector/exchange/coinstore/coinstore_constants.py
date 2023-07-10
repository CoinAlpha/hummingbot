import sys

from hummingbot.core.api_throttler.data_types import RateLimit

# Base URL
REST_URL = "https://api.coinstore.com/api"

WSS_URL = "wss://ws.coinstore.com/s/ws"

# Websocket event types
DIFF_EVENT_TYPE = "depth"
TRADE_EVENT_TYPE = "trade"

# Public API endpoints
LAST_TRADED_PRICE_PATH = "/v1/market/trade/{}"
ORDERBOOK_DEPTH_PATH = "/v1/market/depth/{}"
ALL_SYMBOL_PATH = "/v1/ticker/price"

WS_HEARTBEAT_TIME_INTERVAL = 30

# Rate Limit
NO_LIMIT = sys.maxsize

RATE_LIMITS = [
    # General
    RateLimit(limit_id=ALL_SYMBOL_PATH, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=ORDERBOOK_DEPTH_PATH, limit=NO_LIMIT, time_interval=1),
    RateLimit(limit_id=LAST_TRADED_PRICE_PATH, limit=NO_LIMIT, time_interval=1),
]
