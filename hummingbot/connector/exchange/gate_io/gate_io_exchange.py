from typing import List, Optional

from hummingbot.connector.exchange.gate_io import (
    gate_io_constants as CONSTANTS,
)
from hummingbot.connector.exchange_base_v2 import ExchangeBaseV2
from hummingbot.connector.exchange.gate_io.gate_io_exchange_api import GateIoExchangeApi
from hummingbot.core.data_type.common import OrderType


class GateIoExchange(ExchangeBaseV2):
    DEFAULT_DOMAIN = ""
    SUPPORTED_ORDER_TYPES = [
        OrderType.LIMIT
    ]
    HBOT_ORDER_ID_PREFIX = CONSTANTS.HBOT_ORDER_ID
    MAX_ORDER_ID_LEN = CONSTANTS.MAX_ID_LEN

    EXCHANGE_API_CLASS = GateIoExchangeApi

    def __init__(self,
                 gate_io_api_key: str,
                 gate_io_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = DEFAULT_DOMAIN):
        """
        :param gate_io_api_key: The API key to connect to private Gate.io APIs.
        :param gate_io_secret_key: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        self._auth_credentials = {
            "gate_io_api_key": gate_io_api_key,
            "gate_io_secret_key": gate_io_secret_key,
        }
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__()

    @property
    def name(self) -> str:
        return "gate_io"
