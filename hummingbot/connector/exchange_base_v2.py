import logging
import asyncio
from typing import Any, AsyncIterable, Dict, List, Optional
from decimal import Decimal

from async_timeout import timeout

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.constants import s_decimal_NaN, s_decimal_0
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, OrderState, TradeUpdate
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeBase, TokenAmount
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger


class ExchangeBaseV2(ExchangeBase):
    _logger = None

    # The following class vars MUST be redefined in the child class
    # because they cannot have defaults, so we keep them commented
    # to cause a NameError if not defined
    #
    # DEFAULT_DOMAIN = ""
    # SUPPORTED_ORDER_TYPES = []
    # MAX_ORDER_ID_LEN = None
    # HBOT_ORDER_ID_PREFIX = None
    # EXCHANGE_API_CLASS = None

    def __init__(self):
        super().__init__()
        self._last_timestamp = 0
        self._api = self.EXCHANGE_API_CLASS(
            self,  # TODO bad
            self._auth_credentials,
            self._trading_pairs,
            self._trading_required,
            self._domain,
        )
        self._api_listener_task = None
        self._order_tracker: ClientOrderTracker = ClientOrderTracker(connector=self)
        # TODO
        self._trading_rules = self._api._trading_rules

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def name_cap(self) -> str:
        return self.name.capitalize()

    # Implementation-specific method
    #
    def name(self):
        raise NotImplementedError

    # Price logic
    #
    @staticmethod
    def quantize_value(value: Decimal, quantum: Decimal) -> Decimal:
        return (value // quantum) * quantum

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        if price.is_nan():
            return price
        price_quantum = self.get_order_price_quantum(trading_pair, price)
        return ExchangeBaseV2.quantize_value(price, price_quantum)

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

    # Exchange / Trading logic
    #
    def supported_order_types(self):
        return self.SUPPORTED_ORDER_TYPES

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._api.order_book_tracker.order_books

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

    def get_order_book(self, trading_pair: str) -> OrderBook:
        """
        Returns the current order book for a particular market

        :param trading_pair: the pair of tokens for which the order book should be retrieved
        """
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    # Order Tracking
    #
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
                             order_type: OrderType):
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

    # Buy / Sell / Cancel Orders
    # Get fee
    #
    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.MARKET,
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
            hbot_order_id_prefix=self.HBOT_ORDER_ID_PREFIX,
            max_id_len=self.MAX_ORDER_ID_LEN
        )
        safe_ensure_future(self._create_order(
            trade_type=TradeType.BUY,
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price))
        return order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.MARKET,
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
            hbot_order_id_prefix=self.HBOT_ORDER_ID_PREFIX,
            max_id_len=self.MAX_ORDER_ID_LEN
        )
        safe_ensure_future(self._create_order(
            trade_type=TradeType.SELL,
            order_id=order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price))
        return order_id

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        Calculates the fee to pay based on the fee information provided by the exchange for the account and the token pair.
        If exchange info is not available it calculates the estimated fee an order would pay based on the connector
            configuration

        :param base_currency: the order base currency
        :param quote_currency: the order quote currency
        :param order_type: the type of order (MARKET, LIMIT, LIMIT_MAKER)
        :param order_side: if the order is for buying or selling
        :param amount: the order amount
        :param price: the order price
        :param is_maker: True if the order is a maker order, False if it is a taker order

        :return: the calculated or estimated fee
        """
        try:
            return self._api.get_fee(base_currency, quote_currency, order_type, order_side, amount, price, is_maker)
        except NotImplementedError:
            is_maker = order_type is OrderType.LIMIT_MAKER
            return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    def cancel(self, trading_pair: str, order_id: str):
        """
        Creates a promise to cancel an order in the exchange

        :param trading_pair: the trading pair the order to cancel operates with
        :param order_id: the client id of the order to cancel

        :return: the client id of the order to cancel
        """
        safe_ensure_future(self._cancel_order(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all currently active orders. The cancellations are performed in parallel tasks.

        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run

        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        tasks = [self._cancel_order(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cr in cancellation_results:
                    if isinstance(cr, Exception):
                        continue

                    client_order_id = None
                    if cr is not None:
                        if not client_order_id:
                            client_order_id = cr

                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection."
            )
        failed_cancellations = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellations + failed_cancellations

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Optional[Decimal] = None):
        """
        Creates a an order in the exchange using the parameters to configure it

        :param trade_type: the side of the order (BUY of SELL)
        :param order_id: the id that should be assigned to the order (the client id)
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        """
        exchange_order_id = ""
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
            amount=amount
        )

        def order_failed():
            order_update: OrderUpdate = OrderUpdate(
                client_order_id=order_id,
                trading_pair=trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState.FAILED,
            )
            self._order_tracker.process_order_update(order_update)

        if order_type not in self.SUPPORTED_ORDER_TYPES:
            self.logger().error(f"{order_type} not in SUPPORTED_ORDER_TYPES")
            order_failed()
            return

        if amount < trading_rule.min_order_size:
            self.logger().warning(f"{trade_type.name.title()} order amount {amount} is lower than the minimum order"
                                  f" size {trading_rule.min_order_size}. The order will not be created.")
            order_failed()
            return

        try:
            exchange_order_id, update_timestamp = await self._api.create_order(
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                trade_type=trade_type,
                order_type=order_type,
                price=price)

            order_update: OrderUpdate = OrderUpdate(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                update_timestamp=update_timestamp,
                new_state=OrderState.OPEN,
            )
            self._order_tracker.process_order_update(order_update)

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().network(
                f"Error submitting {trade_type.name.lower()} {order_type.name.upper()} order to {self.name_cap} for "
                f"{amount} {trading_pair} {price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to {self.name_cap}. Check API key and network connection."
            )
            order_update: OrderUpdate = OrderUpdate(
                client_order_id=order_id,
                trading_pair=trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState.FAILED,
            )
            self._order_tracker.process_order_update(order_update)
        return order_id, exchange_order_id

    async def _cancel_order(self, trading_pair: str, order_id: str) -> str:
        """
        Requests the exchange to cancel an active order

        :param trading_pair: the trading pair the order to cancel operates with
        :param order_id: the client id of the order to cancel
        """
        tracked_order = self._order_tracker.fetch_tracked_order(order_id)
        if tracked_order is not None:
            try:
                client_order_id, cancelled = await self._api.cancel_order(order_id, tracked_order)
                if cancelled:
                    order_update: OrderUpdate = OrderUpdate(
                        client_order_id=order_id,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=self.current_timestamp,
                        new_state=OrderState.CANCELLED,
                    )
                    self._order_tracker.process_order_update(order_update)
                    return order_id
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                # Binance does not allow cancels with the client/user order id
                # so log a warning and wait for the creation of the order to complete
                self.logger().warning(
                    f"Failed to cancel the order {order_id} because it does not have an exchange order id yet")
                await self._order_tracker.process_order_not_found(order_id)
            except Exception:
                self.logger().error(
                    f"Failed to cancel order {order_id}", exc_info=True)
        return None

    # lower level API interaction methods
    #
    @property
    def ready(self) -> bool:
        """
        Returns True if the connector is ready to operate (all connections established with the exchange). If it is
        not ready it returns False.
        """
        return all(self._api.status_dict.values())

    @property
    def status_dict(self):
        return self._api.status_dict

    def tick(self, timestamp: float):
        """
        Includes the logic that has to be processed every time a new tick happens in the bot. Particularly it enables
        the execution of the status update polling loop using an event.
        """
        self._api.tick(timestamp)
        self._last_timestamp = timestamp

    async def start_network(self):
        """
        Start all required tasks to update the status of the connector. Those tasks include:
        - The order book tracker
        - The polling loops to update the trading rules and trading fees
        - The polling loop to update order status and balance status using REST API (backup for main update process)
        - The background task to process the events received through the user stream tracker (websocket connection)
        """
        await self._api.start_network()
        self._api_listener_task = safe_ensure_future(self._api_listener_loop())

    def _stop_network(self):
        # Resets timestamps and events for status_polling_loop
        self._last_timestamp = 0
        self._api._stop_network()
        if self._api_listener_task is not None:
            self._api_listener_task.cancel()
            self._api_listener_task = None

    async def stop_network(self):
        """
        This function is executed when the connector is stopped.
        It stops all background tasks that connect to the exchange.
        """
        self._stop_network()

    async def check_network(self):
        """
        Checks connectivity with the exchange using the API.
        """
        return await self._api.check_network()

    async def _api_listener_loop(self):
        async for event in self._iter_api_queue():
            try:
                if event["type"] == "trade":
                    self._process_trade(event)
                if event["type"] == "order":
                    self._process_order(event)
                if event["type"] == "balance":
                    self._process_balance(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in API listener loop. Retrying in 5s.",
                    exc_info=True)
                await self._sleep(5)

    async def _iter_api_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._api.queue.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error while reading API queue. Retrying in 1s.")
                await asyncio.sleep(1)

    def _process_trade(self, trade: Dict[str, Any], client_order_id: Optional[str] = None):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        Example Trade:
        https://www.gate.io/docs/apiv4/en/#retrieve-market-trades
        """
        client_order_id = client_order_id or str(trade["text"])
        tracked_order = self.in_flight_orders.get(client_order_id, None)
        if not tracked_order:
            self.logger().debug(f"Ignoring trade message with id {client_order_id}: not in in_flight_orders.")
            return

        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=tracked_order.trade_type,
            percent_token=trade["fee_currency"],
            flat_fees=[TokenAmount(
                amount=Decimal(trade["fee_amount"]),
                token=trade["fee_currency"]
            )]
        )
        trade_update = TradeUpdate(
            trade_id=str(trade["id"]),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fee=fee,
            fill_base_amount=Decimal(trade["fill_base_amount"]),
            fill_quote_amount=Decimal(trade["fill_quote_amount"]),
            fill_price=Decimal(trade["fill_price"]),
            fill_timestamp=trade["fill_timestamp"],
        )
        self._order_tracker.process_trade_update(trade_update)

    def _process_order(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.

        :param order_msg: The order response from either REST or web socket API (they are of the same format)

        Example Order:
        https://www.gate.io/docs/apiv4/en/#list-orders
        """
        state = None
        client_order_id = str(order_msg.get("text", ""))
        tracked_order = self.in_flight_orders.get(client_order_id, None)
        if not tracked_order:
            self.logger().debug(f"Ignoring order message with id {client_order_id}: not in in_flight_orders.")
            return

        state = self._normalise_order_message_state(order_msg, tracked_order)
        if state:
            order_update = OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=order_msg["update_timestamp"],
                new_state=order_msg["state"],
                client_order_id=client_order_id,
                exchange_order_id=str(order_msg["id"]),
            )
            self._order_tracker.process_order_update(order_update=order_update)
            self.logger().info(f"Successfully updated order {tracked_order.client_order_id}.")

    # TODO
    def _process_balance(self, balance_update):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        for account in balance_update["accounts"]:
            asset_name = account["currency"]
            self._account_available_balances[asset_name] = Decimal(str(account["available"]))
            self._account_balances[asset_name] = Decimal(str(account["locked"])) + Decimal(str(account["available"]))
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    # TODO
    def _process_balance_message_ws(self, balance_update):
        for account in balance_update["accounts"]:
            asset_name = account["currency"]
            self._account_available_balances[asset_name] = Decimal(str(account["available"]))
            self._account_balances[asset_name] = Decimal(str(account["total"]))
