import time
import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional
import json

from hummingbot.connector.exchange_api_base import ExchangeApiBase
from hummingbot.connector.exchange.gate_io import (
    gate_io_constants as CONSTANTS,
    gate_io_web_utils as web_utils
)
from hummingbot.connector.exchange.gate_io.gate_io_auth import GateIoAuth
from hummingbot.connector.exchange.gate_io.gate_io_api_order_book_data_source import GateIoAPIOrderBookDataSource
from hummingbot.connector.exchange.gate_io.gate_io_api_user_stream_data_source import GateIoAPIUserStreamDataSource
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.utils.async_utils import safe_gather


class GateIoExchangeApi(ExchangeApiBase):
    DEFAULT_DOMAIN = ""
    RATE_LIMITS = CONSTANTS.RATE_LIMITS

    ORDERBOOK_DS_CLASS = GateIoAPIOrderBookDataSource
    USERSTREAM_DS_CLASS = GateIoAPIUserStreamDataSource

    CHECK_NETWORK_URL = CONSTANTS.NETWORK_CHECK_PATH_URL
    SYMBOLS_PATH_URL = CONSTANTS.SYMBOL_PATH_URL
    FEE_PATH_URL = SYMBOLS_PATH_URL

    INTERVAL_TRADING_RULES = CONSTANTS.INTERVAL_TRADING_RULES
    # Using 120 seconds here as Gate.io websocket is quiet
    TICK_INTERVAL_LIMIT = 10.0  # 120.0

    web_utils = web_utils

    # Defined in __init__
    # TODO
    USER_CHANNELS = {}

    def __init__(self,
                 exchange,
                 auth_credentials: {},
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = DEFAULT_DOMAIN
                 ):
        """
        :param gate_io_api_key: The API key to connect to private Gate.io APIs.
        :param gate_io_secret_key: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        # TODO this is bad, cross references mean wrong decoupling
        # acceptable given it's very limited and the benefits
        # but better to solve, see below order_books() property comments
        self.exchange = exchange

        self._auth_credentials = auth_credentials
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self.USER_CHANNELS = (
            (CONSTANTS.USER_TRADES_ENDPOINT_NAME, self.enqueue_trade),
            (CONSTANTS.USER_ORDERS_ENDPOINT_NAME, self.enqueue_order),
            (CONSTANTS.USER_BALANCE_ENDPOINT_NAME, self.enqueue_balance),
        )
        super().__init__()

    # TODO understand how to cleanly decouple Exchange and Api
    # regarding order books, balances, in flight orders
    # Probably the best thing is to have an ExchangeBooks object
    # that contains the shared state(s) to limit cross references
    # to what is really needed
    #
    # OrderBook is a hbot class and should be in the Exchange / ExchangeBooks
    @property
    def order_books(self) -> Dict:
        return self._api.order_book_tracker.order_books

    def init_auth(self):
        # TODO improve
        self._auth_credentials["time_provider"] = self._time_synchronizer
        return GateIoAuth(**self._auth_credentials)

    @property
    def name(self) -> str:
        return "gate_io"

    async def _polling_status_fetch_updates(self):
        """
        Called by _polling_status_loop, which executes after each tick() is executed
        """
        self.logger().debug(f"Running _status_polling_loop_fetch_updates() at {time.time()}")
        return await safe_gather(
            self._polling_balances(),
            self._polling_orders(),
        )

    async def _polling_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        account_info = ""
        try:
            account_info = await self._api_get(
                path_url=CONSTANTS.USER_BALANCES_PATH_URL,
                is_auth_required=True,
                limit_id=CONSTANTS.USER_BALANCES_PATH_URL
            )
            await self.enqueue_balance(account_info)
        except Exception as e:
            self.logger().network(
                f"Unexpected error while fetching balance update - {str(e)}", exc_info=True,
                app_warning_msg=(f"Could not fetch balance update from {self.name_cap}"))
        return account_info

    async def _polling_orders(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        orders_tasks = []
        trades_tasks = []
        reviewed_orders = []
        tracked_orders = list(self.exchange.in_flight_orders.values())
        if len(tracked_orders) <= 0:
            return

        # Prepare requests to update trades and orders
        for tracked_order in tracked_orders:
            try:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                reviewed_orders.append(tracked_order)
            except asyncio.TimeoutError:
                self.logger().network(
                    f"Skipped order status update for {tracked_order.client_order_id} "
                    "- waiting for exchange order id.")
                await self._order_tracker.process_order_not_found(tracked_order.client_order_id)
                continue

            trading_pair = self.web_utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)
            trades_tasks.append(self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={
                    "currency_pair": trading_pair,
                    "order_id": exchange_order_id
                },
                is_auth_required=True,
                limit_id=CONSTANTS.MY_TRADES_PATH_URL,
            ))
            orders_tasks.append(self._api_get(
                path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(order_id=exchange_order_id),
                params={
                    "currency_pair": trading_pair
                },
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_STATUS_LIMIT_ID,
            ))

        # Process order trades first before processing order statuses
        responses = await safe_gather(*trades_tasks, return_exceptions=True)
        self.logger().debug(f"Polled trade updates for {len(tracked_orders)} orders: {len(responses)}.")
        for response, tracked_order in zip(responses, reviewed_orders):
            if not isinstance(response, Exception):
                if len(response) > 0:
                    for trade_fills in response:
                        await self.enqueue_trade(trade_fills, tracked_order.client_order_id)
            else:
                self.logger().warning(
                    f"Failed to fetch trade updates for order {tracked_order.client_order_id}. "
                    f"Response: {response}")
                if 'ORDER_NOT_FOUND' in str(response):
                    self._order_tracker.stop_tracking_order(client_order_id=tracked_order.client_order_id)

        # Process order statuses
        responses = await safe_gather(*orders_tasks, return_exceptions=True)
        self.logger().debug(f"Polled order updates for {len(tracked_orders)} orders: {len(responses)}.")
        for response, tracked_order in zip(responses, tracked_orders):
            if not isinstance(response, Exception):
                await self.enqueue_order(response)
            else:
                self.logger().warning(
                    f"Failed to fetch order status updates for order {tracked_order.client_order_id}. "
                    f"Response: {response}")
                if 'ORDER_NOT_FOUND' in str(response):
                    self._order_tracker.stop_tracking_order(client_order_id=tracked_order.client_order_id)

    async def enqueue_trade(self, trades, client_order_id: Optional[str] = None):
        if type(trades) != list:
            trades = list(trades)
        for trade in trades:
            print("TRADE ", trade)
            e = dict(
                type="trade",
                trade_id=trade["id"],
                fee_currency=trade["fee_currency"],
                fee_amount=trade["fee"],
                fill_base_amount=trade["amount"],
                fill_quote_amount=Decimal(trade["amount"]) * Decimal(trade["price"]),
                fill_price=Decimal(trade["price"]),
                fill_timestamp=trade["create_time"]
            )
            e["client_order_id"] = client_order_id
            await self.queue.put(e)

    async def enqueue_order(self, orders):
        if type(orders) != list:
            orders = list(orders)
        for order in orders:
            print("ORDER ", order)
            await self.queue.put(dict(
                type="order",
                order_id=order["id"],
                state=self._normalise_order_message_state(order),
            ))

    async def enqueue_balance(self, balances):
        if type(balances) != list:
            balances = list(balances)
        print("BALANCE", balances)
        await self.queue.put(dict(
            type="balance",
            accounts=balances))

    def _normalise_order_message_state(self, order_msg: Dict[str, Any]):
        state = None
        # we do not handle:
        #   "failed" because it is handled by create order
        #   "put" as the exchange order id is returned in the create order response
        #   "open" for same reason

        # same field for both WS and REST
        amount_left = Decimal(order_msg.get("left"))

        # WS
        if "event" in order_msg:
            event_type = order_msg.get("event")
            if event_type == "update":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.PARTIALLY_FILLED
            if event_type == "finish":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.CANCELLED
        else:
            status = order_msg.get("status")
            if status == "closed":
                state = OrderState.FILLED
                if amount_left > 0:
                    state = OrderState.PARTIALLY_FILLED
            if status == "cancelled":
                state = OrderState.CANCELLED
        return state

    async def _polling_trading_rules(self):
        exchange_info = await self._api_get(path_url=self.SYMBOLS_PATH_URL)
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _format_trading_rules(self, raw_trading_pair_info: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        Converts json API response into a dictionary of trading rules.

        :param raw_trading_pair_info: The json API response
        :return A dictionary of trading rules.

        Example raw_trading_pair_info:
        https://www.gate.io/docs/apiv4/en/#list-all-currency-pairs-supported
        """
        result = []
        for rule in raw_trading_pair_info:
            try:
                trading_pair = self.web_utils.convert_from_exchange_trading_pair(rule["id"])

                min_amount_inc = Decimal(f"1e-{rule['amount_precision']}")
                min_price_inc = Decimal(f"1e-{rule['precision']}")
                min_amount = Decimal(str(rule.get("min_base_amount", min_amount_inc)))
                min_notional = Decimal(str(rule.get("min_quote_amount", min_price_inc)))
                result.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_amount,
                        min_price_increment=min_price_inc,
                        min_base_amount_increment=min_amount_inc,
                        min_notional_size=min_notional,
                        min_order_value=min_notional,
                    )
                )
            except Exception:
                self.logger().error(
                    f"Error parsing the trading pair rule {rule}. Skipping.", exc_info=True)
        return result

    # Exchange API actions
    #
    # TODO do not pass higher level object here
    async def cancel_order(self, order_id, tracked_order):
        """
        Cancels an order via the API

        returns (client_order_id, True) if successful
        """
        cancelled = False
        exchange_order_id = await tracked_order.get_exchange_order_id()
        try:
            params = {
                'currency_pair': self.web_utils.convert_to_exchange_trading_pair(tracked_order.trading_pair)
            }
            resp = await self._api_delete(
                path_url=CONSTANTS.ORDER_DELETE_PATH_URL.format(order_id=exchange_order_id),
                params=params,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDER_DELETE_LIMIT_ID,
            )
            if resp["status"] == "cancelled":
                cancelled = True
        except (asyncio.TimeoutError, self.web_utils.APIError) as e:
            self.logger().debug(f"Canceling order {order_id} failed: {e}")
        return (order_id, cancelled)

    async def create_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal) -> str:

        order_type_str = order_type.name.lower().split("_")[0]
        symbol = self.web_utils.convert_to_exchange_trading_pair(trading_pair)
        data = {
            "text": order_id,
            "currency_pair": symbol,
            "side": trade_type.name.lower(),
            "type": order_type_str,
            "price": f"{price:f}",
            "amount": f"{amount:f}",
        }
        # RESTRequest does not support json, and if we pass a dict
        # the underlying aiohttp will encode it to params
        data = json.dumps(data)
        endpoint = CONSTANTS.ORDER_CREATE_PATH_URL
        order_result = await self._api_post(
            path_url=endpoint,
            data=data,
            is_auth_required=True,
            limit_id=endpoint,
        )
        if order_result.get('status') in {"cancelled", "expired", "failed"}:
            raise self.web_utils.APIError({'label': 'ORDER_REJECTED', 'message': 'Order rejected.'})
        exchange_order_id = str(order_result["id"])
        return exchange_order_id, time.time()

    async def get_api_message(self):
        return await self._api.queue.get()
