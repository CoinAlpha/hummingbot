from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils
from hummingbot.connector.exchange.coinstore.coinstore_api_order_book_data_source import CoinstoreAPIOrderBookDataSource
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CoinstoreExchange(ExchangePyBase):

    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
    ) -> None:
        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required
        self._symbol_to_id_map = {}
        super().__init__(client_config_map)

    @property
    def authenticator(self):
        return None

    @property
    def name(self):
        return "coinstore"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return ""

    @property
    def client_order_id_max_length(self):
        return 0

    @property
    def client_order_id_prefix(self):
        return ""

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.ALL_SYMBOL_PATH

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.ALL_SYMBOL_PATH

    @property
    def check_network_request_path(self):
        return CONSTANTS.ALL_SYMBOL_PATH

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # Exchange does not have a particular error for incorrect timestamps
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(auth=self._auth, throttler=self._throttler)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoinstoreAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs, connector=self, api_factory=self._web_assistants_factory
        )

    def _create_user_stream_data_source(self):
        pass

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        err_code = exchange_info.get("code")
        if err_code != 0:
            err_msg: str = f"Error Code: {err_code} - {exchange_info.get('message')}"
            self.logger().error(
                f"Error initializing trading pair symbols with exchange info response. {err_msg} Response: {exchange_info}"
            )
            return

        mapping = bidict()
        data_list: List[Dict[str, Any]] = exchange_info.get("data")

        for symbol_data in data_list:
            exchange_symbol: str = symbol_data["symbol"]
            if exchange_symbol.endswith("USDT"):  # Only supporting USDT for now
                base_asset, _quote_asset = exchange_symbol.split("USDT")
                mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base_asset.upper(), "USDT")
                self._symbol_to_id_map[exchange_symbol] = symbol_data["id"]
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        trading_rules: List[TradingRule] = []
        return trading_rules

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        return "", self._time()

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        return False

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or (order_type in (OrderType.LIMIT_MAKER, OrderType.LIMIT))
        return build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return 0

    async def _update_balances(self):
        pass

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> Optional[OrderUpdate]:
        pass

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        pass
