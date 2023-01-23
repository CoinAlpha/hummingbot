import asyncio
import copy
import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import TYPE_CHECKING, Any, AsyncIterable, Dict, List, Optional, Tuple, Union

from async_timeout import timeout

from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.constants import MINUTE, TWELVE_HOURS, s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order import Order, PlaceOrderResult, CancelOrderResult
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class ExchangePyBase(ExchangeBase, ABC):
    _logger = None

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0
    TRADING_RULES_INTERVAL = 30 * MINUTE
    TRADING_FEES_INTERVAL = TWELVE_HOURS
    TICK_INTERVAL_LIMIT = 60.0

    def __init__(self, client_config_map: "ClientConfigAdapter"):
        super().__init__(client_config_map)

        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._trading_rules = {}
        self._trading_fees = {}

        self._status_polling_task: Optional[asyncio.Task] = None
        self._user_stream_tracker_task: Optional[asyncio.Task] = None
        self._user_stream_event_listener_task: Optional[asyncio.Task] = None
        self._trading_rules_polling_task: Optional[asyncio.Task] = None
        self._trading_fees_polling_task: Optional[asyncio.Task] = None
        self._lost_orders_update_task: Optional[asyncio.Task] = None

        self._time_synchronizer = TimeSynchronizer()
        self._throttler = AsyncThrottler(
            rate_limits=self.rate_limits_rules,
            limits_share_percentage=client_config_map.rate_limits_share_pct)
        self._poll_notifier = asyncio.Event()

        # init Auth and Api factory
        self._auth: AuthBase = self.authenticator
        self._web_assistants_factory: WebAssistantsFactory = self._create_web_assistants_factory()

        # init OrderBook Data Source and Tracker
        self._orderbook_ds: OrderBookTrackerDataSource = self._create_order_book_data_source()
        self._set_order_book_tracker(OrderBookTracker(
            data_source=self._orderbook_ds,
            trading_pairs=self.trading_pairs,
            domain=self.domain))

        # init UserStream Data Source and Tracker
        self._user_stream_tracker = self._create_user_stream_tracker()

        self._order_tracker: ClientOrderTracker = self._create_order_tracker()

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def authenticator(self) -> AuthBase:
        raise NotImplementedError

    @property
    @abstractmethod
    def rate_limits_rules(self) -> List[RateLimit]:
        raise NotImplementedError

    @property
    @abstractmethod
    def domain(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def client_order_id_max_length(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def client_order_id_prefix(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def trading_rules_request_path(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def trading_pairs_request_path(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def check_network_request_path(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def trading_pairs(self) -> List[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_trading_required(self) -> bool:
        raise NotImplementedError

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self.order_book_tracker.order_books

    @property
    def in_flight_orders(self) -> Dict[str, InFlightOrder]:
        return self._order_tracker.active_orders

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self.in_flight_orders.values()
        ]

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized": self.trading_pair_symbol_map_ready(),
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or len(self._account_balances) > 0,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True,
            "user_stream_initialized": self._is_user_stream_initialized(),
        }

    @property
    def ready(self) -> bool:
        """
        Returns True if the connector is ready to operate (all connections established with the exchange). If it is
        not ready it returns False.
        """
        return all(self.status_dict.values())

    @property
    def name_cap(self) -> str:
        return self.name.capitalize()

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        Returns a dictionary associating current active orders client id to their JSON representation
        """
        return {
            key: value.to_json()
            for key, value in self.in_flight_orders.items()
            if not value.is_done
        }

    @abstractmethod
    def supported_order_types(self) -> List[OrderType]:
        raise NotImplementedError

    @abstractmethod
    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        raise NotImplementedError

    async def _place_batch_order_create(
        self, orders_with_kwargs_to_create: List[Tuple[InFlightOrder, Dict[str, Any]]]
    ) -> List[PlaceOrderResult]:
        """Overwrite this method if the exchange allows batch order creation.
        :param orders_with_kwargs_to_create: A list of tuples of in-flight orders to create and order-creation kwargs
            to use in the creation.
        """
        raise NotImplementedError

    async def _place_batch_order_cancel(self, orders_to_cancel: List[InFlightOrder]) -> List[CancelOrderResult]:
        """Overwrite this method if the exchange allows batch order creation.
        :param orders_to_cancel: The in-flight orders to cancel.
        """
        raise NotImplementedError

    # === Price logic ===

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Used by quantize_order_price() in _create_order()
        Returns a price step, a minimum price increment for a given trading pair.

        :param trading_pair: the trading pair to check for market conditions
        :param price: the starting point price
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_price_increment)

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal) -> Decimal:
        """
        Used by quantize_order_price() in _create_order()
        Returns an order amount step, a minimum amount increment for a given trading pair.

        :param trading_pair: the trading pair to check for market conditions
        :param order_size: the starting point order price
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def quantize_order_amount(self, trading_pair: str, amount: Decimal, price: Decimal = s_decimal_0) -> Decimal:
        """
        Applies the trading rules to calculate the correct order amount for the market

        :param trading_pair: the token pair for which the order will be created
        :param amount: the intended amount for the order
        :param price: the intended price for the order

        :return: the quantized order amount after applying the trading rules
        """
        trading_rule = self._trading_rules[trading_pair]
        quantized_amount: Decimal = super().quantize_order_amount(trading_pair, amount)

        # Check against min_order_size and min_notional_size. If not passing either check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        if price == s_decimal_0:
            current_price: Decimal = self.get_price(trading_pair, False)
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount

        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0
        return quantized_amount

    def get_order_book(self, trading_pair: str) -> OrderBook:
        """
        Returns the current order book for a particular market

        :param trading_pair: the pair of tokens for which the order book should be retrieved
        """
        if trading_pair not in self.order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self.order_book_tracker.order_books[trading_pair]

    def tick(self, timestamp: float):
        """
        Includes the logic that has to be processed every time a new tick happens in the bot. Particularly it enables
        the execution of the status update polling loop using an event.
        """
        last_user_stream_message_time = (
            0 if self._user_stream_tracker is None else self._user_stream_tracker.last_recv_time
        )
        last_recv_diff = timestamp - last_user_stream_message_time
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if last_recv_diff > self.TICK_INTERVAL_LIMIT
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            self._poll_notifier.set()
        self._last_timestamp = timestamp

    # === Orders placing ===

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        """
        Creates a promise to create a buy order using the parameters

        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price

        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length
        )
        order = Order(
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=TradeType.BUY,
            amount=amount,
            price=price,
            client_order_id=order_id,
            kwargs=kwargs,
        )
        safe_ensure_future(self._execute_batch_order_create(orders_to_create=[order]))
        return order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.LIMIT,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        """
        Creates a promise to create a sell order using the parameters.
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        :return: the id assigned by the connector to the order (the client id)
        """
        order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length
        )
        order = Order(
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=TradeType.SELL,
            amount=amount,
            price=price,
            client_order_id=order_id,
            kwargs=kwargs,
        )
        safe_ensure_future(self._execute_batch_order_create(orders_to_create=[order]))
        return order_id

    def batch_order_create(self, orders_to_create: List[Order]) -> Tuple[Order]:
        """
        Issues a batch order creation as a single API request for exchanges that implement this feature. The default
        implementation of this method is to send the requests discretely (one by one).
        :param orders_to_create: A list of Order objects representing the orders to create.
        :returns: A tuple composed of the order creation objects, updated with the respective client order ID. If the
            creation was not successful, the ID is not updated.
        """
        creation_result = tuple(orders_to_create)
        for order in creation_result:
            order.client_order_id = get_new_client_order_id(
                is_buy=order.trade_type == TradeType.BUY,
                trading_pair=order.trading_pair,
                hbot_order_id_prefix=self.client_order_id_prefix,
                max_id_len=self.client_order_id_max_length
            )
        safe_ensure_future(self._execute_batch_order_create(orders_to_create=orders_to_create))
        return creation_result

    def batch_order_cancel(self, orders_to_cancel: List[InFlightOrder]):
        """
        Issues a batch order cancelation as a single API request for exchanges that implement this feature. The default
        implementation of this method is to send the requests discretely (one by one).
        :param orders_to_cancel: A list of trading-pair, client order ID tuples.
        """
        safe_ensure_future(coro=self._execute_batch_order_cancel(orders_to_cancel=orders_to_cancel))

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        Calculates the fee to pay based on the fee information provided by the exchange for
        the account and the token pair. If exchange info is not available it calculates the estimated
        fee an order would pay based on the connector configuration.

        :param base_currency: the order base currency
        :param quote_currency: the order quote currency
        :param order_type: the type of order (MARKET, LIMIT, LIMIT_MAKER)
        :param order_side: if the order is for buying or selling
        :param amount: the order amount
        :param price: the order price
        :param is_maker: True if the order is a maker order, False if it is a taker order

        :return: the calculated or estimated fee
        """
        return self._get_fee(base_currency, quote_currency, order_type, order_side, amount, price, is_maker)

    def cancel(self, trading_pair: str, order_id: str):
        """
        Creates a promise to cancel an order in the exchange

        :param trading_pair: the trading pair the order to cancel operates with
        :param order_id: the client id of the order to cancel

        :return: the client id of the order to cancel
        """
        safe_ensure_future(self._execute_batch_cancel(order_ids_to_cancel=[order_id]))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all currently active orders.
        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run
        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        order_ids_to_cancel = [
            order.client_order_id
            for order in self.in_flight_orders.values()
            if not order.is_done
        ]

        try:
            async with timeout(timeout_seconds):
                if len(order_ids_to_cancel) != 0:
                    cancelation_results = await self._execute_batch_cancel(order_ids_to_cancel=order_ids_to_cancel)
                else:
                    cancelation_results = []
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection."
            )
            cancelation_results = [
                CancellationResult(order_id=client_order_id, success=False)
                for client_order_id in order_ids_to_cancel
            ]

        return cancelation_results

    async def _execute_batch_order_create(self, orders_to_create: List[Order]):
        validated_orders_to_create = []
        in_flight_orders_to_create = []
        for order in orders_to_create:
            valid_order = await self._start_tracking_and_validate_order(
                trade_type=order.trade_type,
                order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                amount=order.amount,
                order_type=order.order_type,
                price=order.price,
                **order.kwargs
            )
            if valid_order is not None:
                validated_orders_to_create.append(order)
                in_flight_orders_to_create.append(valid_order)
        try:
            validated_orders_with_kwargs_to_create = list(
                zip(in_flight_orders_to_create, [order.kwargs for order in validated_orders_to_create])
            )
            if len(validated_orders_with_kwargs_to_create) > 1:
                try:
                    place_order_results = await self._place_batch_order_create(
                        orders_with_kwargs_to_create=validated_orders_with_kwargs_to_create
                    )
                except NotImplementedError:  # the exchange does not implement batch order placement
                    place_order_results = await self._place_batch_order_create_discretely(
                        orders_with_kwargs_to_create=validated_orders_with_kwargs_to_create
                    )
            else:  # default discrete calls in order to avoid higher rate limit penalties for batch operations
                place_order_results = await self._place_batch_order_create_discretely(
                    orders_with_kwargs_to_create=validated_orders_with_kwargs_to_create
                )
            for place_order_result, in_flight_order in (
                zip(place_order_results, in_flight_orders_to_create)
            ):
                if place_order_result.success:
                    self._update_order_after_creation_success(
                        exchange_order_id=place_order_result.exchange_order_id,
                        order=in_flight_order,
                        update_timestamp=self.current_timestamp,
                    )
                elif place_order_result.exception:
                    self._on_order_creation_failure(
                        order_id=in_flight_order.client_order_id,
                        trading_pair=in_flight_order.trading_pair,
                        amount=in_flight_order.amount,
                        trade_type=in_flight_order.trade_type,
                        order_type=in_flight_order.order_type,
                        price=in_flight_order.price,
                        exception=place_order_result.exception,
                    )
                else:
                    self._update_order_after_creation_failure(
                        order_id=in_flight_order.client_order_id, trading_pair=in_flight_order.trading_pair
                    )
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            self.logger().network("Batch order create failed.")
            for order in orders_to_create:
                self._on_order_creation_failure(
                    order_id=order.client_order_id,
                    trading_pair=order.trading_pair,
                    amount=order.amount,
                    trade_type=order.trade_type,
                    order_type=order.order_type,
                    price=order.price,
                    exception=ex,
                )

    async def _place_batch_order_create_discretely(
        self, orders_with_kwargs_to_create: List[Tuple[InFlightOrder, Dict[str, Any]]]
    ) -> List[PlaceOrderResult]:
        """
        This method is a default implementation that will issue a separate single-order creation request for each
        of the orders in the list.
        """
        tasks = [
            self._place_order(
                order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                amount=order.amount,
                trade_type=order.trade_type,
                order_type=order.order_type,
                price=order.price,
                **kwargs,
            ) for order, kwargs in orders_with_kwargs_to_create
        ]
        creation_results: List[Union[Exception, Tuple[str, float]]] = await safe_gather(*tasks, return_exceptions=True)

        place_order_results = []
        for i, creation_result in enumerate(creation_results):
            order = orders_with_kwargs_to_create[i][0]
            if isinstance(creation_result, Exception):
                place_order_results.append(
                    PlaceOrderResult(
                        success=False,
                        update_timestamp=self.current_timestamp,
                        client_order_id=order.client_order_id,
                        exchange_order_id=None,
                        trading_pair=order.trading_pair,
                        exception=creation_result,
                    )
                )
            else:
                exchange_order_id, update_timestamp = creation_result
                place_order_results.append(
                    PlaceOrderResult(
                        success=True,
                        update_timestamp=update_timestamp,
                        client_order_id=order.client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=order.trading_pair,
                    )
                )

        return place_order_results

    async def _start_tracking_and_validate_order(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        **kwargs
    ) -> Optional[InFlightOrder]:
        trading_rule = self._trading_rules[trading_pair]

        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            price = self.quantize_order_price(trading_pair, price)
            quantize_amount_price = Decimal("0") if price.is_nan() else price
            amount = self.quantize_order_amount(trading_pair=trading_pair, amount=amount, price=quantize_amount_price)
        else:
            amount = self.quantize_order_amount(trading_pair=trading_pair, amount=amount)

        self.start_tracking_order(
            order_id=order_id,
            exchange_order_id=None,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            **kwargs,
        )
        order = self._order_tracker.active_orders[order_id]

        if order_type not in self.supported_order_types():
            self.logger().error(f"{order_type} is not in the list of supported order types")
            self._update_order_after_creation_failure(order_id=order_id, trading_pair=trading_pair)
            order = None
        elif amount < trading_rule.min_order_size:
            self.logger().warning(f"{trade_type.name.title()} order amount {amount} is lower than the minimum order"
                                  f" size {trading_rule.min_order_size}. The order will not be created.")
            self._update_order_after_creation_failure(order_id=order_id, trading_pair=trading_pair)
            order = None
        elif price is not None and amount * price < trading_rule.min_notional_size:
            self.logger().warning(f"{trade_type.name.title()} order notional {amount * price} is lower than the "
                                  f"minimum notional size {trading_rule.min_notional_size}. "
                                  "The order will not be created.")
            self._update_order_after_creation_failure(order_id=order_id, trading_pair=trading_pair)
            order = None

        return order

    def _update_order_after_creation_success(
        self, exchange_order_id: str, order: InFlightOrder, update_timestamp: float
    ):
        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(exchange_order_id),
            trading_pair=order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=OrderState.OPEN,
        )
        self._order_tracker.process_order_update(order_update)

    def _on_order_creation_failure(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Optional[Decimal],
        exception: Exception,
        **kwargs,
    ):
        self.logger().network(
            f"Error submitting {trade_type.name.lower()} {order_type.name.upper()} order to {self.name_cap} for "
            f"{amount} {trading_pair} {price}.",
            exc_info=True,
            app_warning_msg=f"Failed to submit buy order to {self.name_cap}. Check API key and network connection."
        )
        self._update_order_after_creation_failure(order_id=order_id, trading_pair=trading_pair)

    def _update_order_after_creation_failure(self, order_id: str, trading_pair: str):
        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order_id,
            trading_pair=trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.FAILED,
        )
        self._order_tracker.process_order_update(order_update)

    async def _execute_batch_cancel(self, order_ids_to_cancel: List[str]) -> List[CancellationResult]:
        results = []
        tracked_orders_to_cancel = []

        for order_id in order_ids_to_cancel:
            tracked_order = self._order_tracker.fetch_tracked_order(client_order_id=order_id)
            if tracked_order is not None:
                tracked_orders_to_cancel.append(tracked_order)
            else:
                results.append(CancellationResult(order_id=order_id, success=False))

        results.extend(await self._execute_batch_order_cancel(orders_to_cancel=tracked_orders_to_cancel))

        return results

    async def _execute_batch_order_cancel(self, orders_to_cancel: List[InFlightOrder]) -> List[CancellationResult]:
        try:
            if len(orders_to_cancel) > 1:
                try:
                    cancel_order_results = await self._place_batch_order_cancel(orders_to_cancel=orders_to_cancel)
                except NotImplementedError:  # the exchange does not implement batch order placement
                    cancel_order_results = await self._place_batch_order_cancel_discretely(
                        orders_to_cancel=orders_to_cancel
                    )
            else:  # default to discrete calls in order to avoid higher rate limit penalties for batch operations
                cancel_order_results = await self._place_batch_order_cancel_discretely(
                    orders_to_cancel=orders_to_cancel
                )
            cancelation_results = [
                CancellationResult(order_id=cancel_order_result.client_order_id, success=cancel_order_result.success)
                for cancel_order_result in cancel_order_results
            ]
            for cancel_order_result in cancel_order_results:
                if cancel_order_result.success:
                    order_update: OrderUpdate = OrderUpdate(
                        client_order_id=cancel_order_result.client_order_id,
                        trading_pair=cancel_order_result.trading_pair,
                        update_timestamp=self.current_timestamp,
                        new_state=(OrderState.CANCELED
                                   if self.is_cancel_request_in_exchange_synchronous
                                   else OrderState.PENDING_CANCEL),
                    )
                    self._order_tracker.process_order_update(order_update)
                elif cancel_order_result.not_found:
                    self.logger().warning(
                        f"Failed to cancel the order {cancel_order_result.client_order_id} due to the order"
                        f" not being found."
                    )
                    await self._order_tracker.process_order_not_found(
                        client_order_id=cancel_order_result.client_order_id
                    )
                elif cancel_order_result.exception is not None:
                    self.logger().error(
                        f"Failed to cancel order {cancel_order_result.client_order_id}",
                        exc_info=cancel_order_result.exception,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                f"Failed to cancel orders {', '.join([o.client_order_id for o in orders_to_cancel])}",
                exc_info=True,
            )
            cancelation_results = [
                CancellationResult(order_id=order.client_order_id, success=False)
                for order in orders_to_cancel
            ]

        return cancelation_results

    async def _place_batch_order_cancel_discretely(
        self, orders_to_cancel: List[InFlightOrder]
    ) -> List[CancelOrderResult]:
        """
        This method is a default implementation that will issue a separate single-order cancelation request for each
        of the orders in the list.
        """
        tasks = [
            self._place_cancel(order_id=order.client_order_id, tracked_order=order)
            for order in orders_to_cancel
        ]
        cancelation_results: List[Union[Exception, bool]] = await safe_gather(*tasks, return_exceptions=True)

        cancel_order_results = [
            CancelOrderResult(
                success=result if not isinstance(result, Exception) else False,
                client_order_id=order.client_order_id,
                trading_pair=order.trading_pair,
                exception=result if isinstance(result, Exception) else None,
                not_found=isinstance(result, asyncio.TimeoutError),
            ) for result, order in zip(cancelation_results, orders_to_cancel)
        ]

        return cancel_order_results

    def _update_order_after_cancelation_success(self, order: InFlightOrder):
        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order.client_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=(OrderState.CANCELED
                       if self.is_cancel_request_in_exchange_synchronous
                       else OrderState.PENDING_CANCEL),
        )
        self._order_tracker.process_order_update(order_update)

    # === Order Tracking ===

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.

        :param saved_states: The saved tracking_states.
        """
        self._order_tracker.restore_tracking_states(tracking_states=saved_states)

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: Optional[str],
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType,
                             **kwargs):
        """
        Starts tracking an order by adding it to the order tracker.

        :param order_id: the order identifier
        :param exchange_order_id: the identifier for the order in the exchange
        :param trading_pair: the token pair for the operation
        :param trade_type: the type of order (buy or sell)
        :param price: the price for the order
        :param amount: the amount for the order
        :param order_type: type of execution for the order (MARKET, LIMIT, LIMIT_MAKER)
        """
        self._order_tracker.start_tracking_order(
            InFlightOrder(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                amount=amount,
                price=price,
                creation_timestamp=self.current_timestamp
            )
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order

        :param order_id: The id of the order that will not be tracked any more
        """
        self._order_tracker.stop_tracking_order(client_order_id=order_id)

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)

    # === Implementation-specific methods ===

    @abstractmethod
    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        raise NotImplementedError

    @abstractmethod
    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs,
                           ) -> Tuple[str, float]:
        raise NotImplementedError

    @abstractmethod
    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        raise NotImplementedError

    # === Network-API-related code ===

    # overridden in implementation of exchanges
    #
    web_utils = None

    async def start_network(self):
        """
        Start all required tasks to update the status of the connector. Those tasks include:
        - The order book tracker
        - The polling loops to update the trading rules and trading fees
        - The polling loop to update order status and balance status using REST API (backup for main update process)
        - The background task to process the events received through the user stream tracker (websocket connection)
        """
        self._stop_network()
        self.order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        self._trading_fees_polling_task = safe_ensure_future(self._trading_fees_polling_loop())
        if self.is_trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = self._create_user_stream_tracker_task()
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
            self._lost_orders_update_task = safe_ensure_future(self._lost_orders_update_polling_loop())

    async def stop_network(self):
        """
        This function is executed when the connector is stopped. It perform a general cleanup and stops all background
        tasks that require the connection with the exchange to work.
        """
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        """
        Checks connectivity with the exchange using the API
        """
        try:
            await self._make_network_check_request()
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def _stop_network(self):
        # Resets timestamps and events for status_polling_loop
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._poll_notifier = asyncio.Event()

        self.order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._trading_fees_polling_task is not None:
            self._trading_fees_polling_task.cancel()
            self._trading_fees_polling_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None
        if self._lost_orders_update_task is not None:
            self._lost_orders_update_task.cancel()
            self._lost_orders_update_task = None

    # === loops and sync related methods ===
    #
    async def _trading_rules_polling_loop(self):
        """
        Updates the trading rules by requesting the latest definitions from the exchange.
        Executes regularly every 30 minutes
        """
        while True:
            try:
                await safe_gather(self._update_trading_rules())
                await self._sleep(self.TRADING_RULES_INTERVAL)
            except NotImplementedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching trading rules.", exc_info=True,
                    app_warning_msg=f"Could not fetch new trading rules from {self.name_cap}"
                                    " Check network connection.")
                await self._sleep(0.5)

    async def _trading_fees_polling_loop(self):
        """
        Only some exchanges provide a fee endpoint.
        If _update_trading_fees() is not defined, we just exit the loop
        """
        while True:
            try:
                await safe_gather(self._update_trading_fees())
                await self._sleep(self.TRADING_FEES_INTERVAL)
            except NotImplementedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching trading fees.", exc_info=True,
                    app_warning_msg=f"Could not fetch new trading fees from {self.name_cap}."
                                    " Check network connection.")
                await self._sleep(0.5)

    async def _status_polling_loop(self):
        """
        Performs all required operation to keep the connector updated and synchronized with the exchange.
        It contains the backup logic to update status using API requests in case the main update source
        (the user stream data source websocket) fails.
        It also updates the time synchronizer. This is necessary because the exchange requires
        the time of the client to be the same as the time in the exchange.
        Executes when the _poll_notifier event is enabled by the `tick` function.
        """
        while True:
            try:
                await self._poll_notifier.wait()
                await self._update_time_synchronizer()

                # the following method is implementation-specific
                await self._status_polling_loop_fetch_updates()

                self._last_poll_timestamp = self.current_timestamp
                self._poll_notifier = asyncio.Event()
            except asyncio.CancelledError:
                raise
            except NotImplementedError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching account updates.",
                    exc_info=True,
                    app_warning_msg=f"Could not fetch account updates from {self.name_cap}. "
                                    "Check API key and network connection.")
                await self._sleep(0.5)

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        try:
            await self._time_synchronizer.update_server_time_offset_with_time_provider(
                time_provider=self.web_utils.get_current_server_time(
                    throttler=self._throttler,
                    domain=self.domain,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            if not pass_on_non_cancelled_error:
                self.logger().exception(f"Error requesting time from {self.name_cap} server")
                raise

    async def _lost_orders_update_polling_loop(self):
        """
        This loop regularly executes the update of lost orders, to keep receiving any new order fill or status change
        until we are totally sure the order is no longer alive in the exchange
        """
        while True:
            try:
                await self._cancel_lost_orders()
                await self._update_lost_orders_status()
                await self._sleep(self.SHORT_POLL_INTERVAL)
            except NotImplementedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error while updating the time synchronizer")
                await self._sleep(0.5)

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        """
        Called by _user_stream_event_listener.
        """
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error while reading user events queue. Retrying in 1s.")
                await self._sleep(1.0)

    def _is_user_stream_initialized(self):
        return self._user_stream_tracker.data_source.last_recv_time > 0 or not self.is_trading_required

    def _create_user_stream_tracker(self):
        return UserStreamTracker(data_source=self._create_user_stream_data_source())

    def _create_user_stream_tracker_task(self):
        return safe_ensure_future(self._user_stream_tracker.start())

    # === Exchange / Trading logic methods that call the API ===

    async def _update_trading_rules(self):
        exchange_info = await self._make_trading_rules_request()
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    async def _api_get(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.GET
        return await self._api_request(*args, **kwargs)

    async def _api_post(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.POST
        return await self._api_request(*args, **kwargs)

    async def _api_put(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.PUT
        return await self._api_request(*args, **kwargs)

    async def _api_delete(self, *args, **kwargs):
        kwargs["method"] = RESTMethod.DELETE
        return await self._api_request(*args, **kwargs)

    async def _api_request_url(self, path_url: str, is_auth_required: bool = False) -> str:
        if is_auth_required:
            url = self.web_utils.private_rest_url(path_url, domain=self.domain)
        else:
            url = self.web_utils.public_rest_url(path_url, domain=self.domain)

        return url

    async def _api_request(
            self,
            path_url,
            overwrite_url: Optional[str] = None,
            method: RESTMethod = RESTMethod.GET,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            is_auth_required: bool = False,
            return_err: bool = False,
            limit_id: Optional[str] = None,
            **kwargs,
    ) -> Dict[str, Any]:

        last_exception = None
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)

        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    data=data,
                    method=method,
                    is_auth_required=is_auth_required,
                    return_err=return_err,
                    throttler_limit_id=limit_id if limit_id else path_url,
                )

                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                if self._is_request_exception_related_to_time_synchronizer(request_exception=request_exception):
                    self._time_synchronizer.clear_time_offset_ms_samples()
                    await self._update_time_synchronizer()
                else:
                    raise

        # Failed even after the last retry
        raise last_exception

    async def _status_polling_loop_fetch_updates(self):
        """
        Called by _status_polling_loop, which executes after each tick() is executed
        """
        await safe_gather(
            self._update_all_balances(),
            self._update_order_status(),
        )

    async def _update_all_balances(self):
        await self._update_balances()
        if not self.real_time_balance_update:
            # This is only required for exchanges that do not provide balance update notifications through websocket
            self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self.in_flight_orders.items()}
            self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        for order in orders:
            try:
                trade_updates = await self._all_trade_updates_for_order(order=order)
                for trade_update in trade_updates:
                    self._order_tracker.process_trade_update(trade_update)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates for order {order.client_order_id}. Error: {request_error}")

    async def _update_orders(self):
        orders_to_update = self.in_flight_orders.copy()
        for client_order_id, order in orders_to_update.items():
            try:
                order_update = await self._request_order_status(tracked_order=order)
                if client_order_id in self.in_flight_orders:
                    self._order_tracker.process_order_update(order_update)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                self.logger().debug(
                    f"Tracked order {client_order_id} does not have an exchange id. "
                    f"Attempting fetch in next polling interval."
                )
                await self._order_tracker.process_order_not_found(client_order_id)
            except Exception as request_error:
                self.logger().network(
                    f"Error fetching status update for the order {order.client_order_id}: {request_error}.",
                    app_warning_msg=f"Failed to fetch status update for the order {order.client_order_id}.",
                )
                await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _update_lost_orders(self):
        orders_to_update = self._order_tracker.lost_orders.copy()
        for client_order_id, order in orders_to_update.items():
            try:
                order_update = await self._request_order_status(tracked_order=order)
                if client_order_id in self._order_tracker.lost_orders:
                    self._order_tracker.process_order_update(order_update)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Error fetching status update for lost order {order.client_order_id}: {request_error}.")

    async def _update_order_status(self):
        await self._update_orders_fills(orders=list(self._order_tracker.all_fillable_orders.values()))
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_orders_fills(orders=list(self._order_tracker.lost_orders.values()))
        await self._update_lost_orders()

    async def _cancel_lost_orders(self):
        orders_to_cancel = list(self._order_tracker.lost_orders.values())
        if len(orders_to_cancel) != 0:
            await self._execute_batch_order_cancel(orders_to_cancel=orders_to_cancel)

    # Methods tied to specific API data formats
    #
    @abstractmethod
    async def _update_trading_fees(self):
        raise NotImplementedError

    @abstractmethod
    async def _user_stream_event_listener(self):
        raise NotImplementedError

    @abstractmethod
    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        raise NotImplementedError

    @abstractmethod
    async def _update_balances(self):
        raise NotImplementedError

    @abstractmethod
    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        raise NotImplementedError

    @abstractmethod
    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        raise NotImplementedError

    @abstractmethod
    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        raise NotImplementedError

    @abstractmethod
    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        raise NotImplementedError

    @abstractmethod
    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        raise NotImplementedError

    @abstractmethod
    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        raise NotImplementedError

    def _create_order_tracker(self) -> ClientOrderTracker:
        return ClientOrderTracker(connector=self)

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._make_trading_pairs_request()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path)

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        return exchange_info
