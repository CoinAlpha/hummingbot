"""
Decibel Perpetual Connector Constants
"""
from decimal import Decimal

DEFAULT_DOMAIN = "decibel_perpetual"

# API Endpoints
BASE_REST_URL = "https://api.decibel.exchange"
BASE_WS_URL = "wss://ws.decibel.exchange"

# API Version
API_VERSION = "v1"

# Intervals
SECONDS_NULL = float("nan")
MIN_POLL_INTERVAL = 5.0
POLL_INTERVAL = 1.0

# Timeouts
MESSAGE_TIMEOUT = 30.0
PING_TIMEOUT = 10.0

# Trading Rules
MIN_ORDER_SIZE = Decimal("0.01")
MAX_ORDER_SIZE = Decimal("1000000")
MIN_PRICE = Decimal("0.0001")
MAX_PRICE = Decimal("1000000")
PRICE_TICK_SIZE = Decimal("0.0001")
SIZE_TICK_SIZE = Decimal("0.01")

# Fees
MAKER_FEE = Decimal("0.0002")  # 0.02%
TAKER_FEE = Decimal("0.0005")  # 0.05%

# Symbols
BASE_QUOTE_ASSET = "USDT"
DEFAULT_ASSET_PAIR = "BTC-USDT"

# Exchange metadata
EXCHANGE_NAME = "decibel"
DISPLAY_NAME = "Decibel Perpetual"
SUPPORTED_ORDER_TYPES = ["limit", "market"]
SUPPORTED_POSITION_MODES = ["one_way", "hedge"]

# Rate Limits
RATE_LIMITS = {
    "rest_public": {
        "limit": 100,
        "time": 60,  # 100 requests per minute
    },
    "rest_private": {
        "limit": 50,
        "time": 60,  # 50 requests per minute
    },
    "ws_public": {
        "limit": 100,
        "time": 60,  # 100 messages per minute
    },
    "ws_private": {
        "limit": 50,
        "time": 60,  # 50 messages per minute
    },
}
