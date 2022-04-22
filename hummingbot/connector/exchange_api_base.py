import logging
import asyncio
from typing import Any, AsyncIterable, Dict, Optional

from hummingbot.connector.constants import MINUTE, TWELVE_HOURS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger


class ExchangeApiBase(object):
    _logger = None

    # The following class vars MUST be redefined in the child class
    # because they cannot have defaults, so we keep them commented
    # to cause a NameError if not defined
    #

    # RATE_LIMITS = None
    # SYMBOLS_PATH_URL = ""
    # FEE_PATH_URL = ""
    # CHECK_NETWORK_URL = ""
    # ORDERBOOK_DS_CLASS = None
    # USERSTREAM_DS_CLASS = None

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0
    TRADING_RULES_INTERVAL = 30 * MINUTE
    TRADING_FEES_INTERVAL = TWELVE_HOURS
    UPDATE_ORDERS_INTERVAL = 10.0
    TICK_INTERVAL_LIMIT = 60.0

    web_utils = None

    def __init__(self):
        self._trading_rules = {}

        self._last_poll_timestamp = 0
        self._last_timestamp = 0

        self._polling_task = None
        self._trading_rules_polling_task = None
        self._trading_fees_polling_task = None

        self._user_stream_event_listener_task = None
        self._user_stream_tracker_task = None

        self._time_synchronizer = TimeSynchronizer()
        self._throttler = AsyncThrottler(self.RATE_LIMITS)
        self._poll_notifier = asyncio.Event()

        # all events (WS and polling) coming from API to Exchange
        self.queue: asyncio.Queue = asyncio.Queue()

        # init Auth and Api factory
        self._auth = self.init_auth()
        self._api_factory = self.web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

        # init OrderBook Data Source and Tracker
        self._orderbook_ds = self.ORDERBOOK_DS_CLASS(
            trading_pairs=self._trading_pairs,
            domain=self._domain,
            api_factory=self._api_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer)
        self._order_book_tracker = OrderBookTracker(
            data_source=self._orderbook_ds,
            trading_pairs=self._trading_pairs,
            domain=self._domain)

        # init UserStream Data Source and Tracker
        self._userstream_ds = self.USERSTREAM_DS_CLASS(
            trading_pairs=self._trading_pairs,
            auth=self._auth,
            domain=self._domain,
            api_factory=self._api_factory,
            throttler=self._throttler)
        self._user_stream_tracker = UserStreamTracker(
            data_source=self._userstream_ds)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def name_cap(self) -> str:
        return self.name.capitalize()

    def name(self):
        raise NotImplementedError

    async def _sleep(self, delay: float):
        await asyncio.sleep(delay)

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized":
                # TODO
                self._orderbook_ds.trading_pair_symbol_map_ready(domain=self._domain),
            "user_stream_initialized":
                # TODO
                self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
            "order_books_initialized":
                self._order_book_tracker.ready,
            "account_balance":
                not self._trading_required or len(self._account_balances) > 0,
            "trading_rule_initialized":
                len(self._trading_rules) > 0 if self._trading_required else True,
        }

    def tick(self, timestamp: float):
        """
        Includes the logic that has to be processed every time a new tick happens in the bot. Particularly it enables
        the execution of the status update polling loop using an event.
        """
        last_recv_diff = timestamp - self._user_stream_tracker.last_recv_time
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if last_recv_diff > self.TICK_INTERVAL_LIMIT
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def start_network(self):
        """
        Start all required tasks to update the status of the connector. Those tasks include:
        - The order book tracker
        - The polling loops to update the trading rules and trading fees
        - The polling loop to update order status and balance status using REST API (backup for main update process)
        - The background task to process the events received through the user stream tracker (websocket connection)
        """
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._polling_trading_rules_loop())
        self._trading_fees_polling_task = safe_ensure_future(self._polling_trading_fees_loop())
        if self._trading_required:
            self._polling_task = safe_ensure_future(self._polling_status_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    def _stop_network(self):
        # Resets timestamps and events for status_polling_loop
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._poll_notifier = asyncio.Event()

        self._order_book_tracker.stop()
        if self._polling_task is not None:
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
            await self._api_get(path_url=self.CHECK_NETWORK_URL)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    # loops and sync related methods
    #
    async def _polling_trading_rules_loop(self):
        """
        Updates the trading rules by requesting the latest definitions from the exchange.
        Executes regularly every 30 minutes
        """
        while True:
            try:
                await safe_gather(self._polling_trading_rules())
                await self._sleep(self.TRADING_RULES_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching trading rules.", exc_info=True,
                    app_warning_msg=f"Could not fetch new trading rules from {self.name_cap}"
                                    " Check network connection.")
                await self._sleep(0.5)

    async def _polling_trading_fees_loop(self):
        """
        Only some exchanges provide a fee endpoint.
        If _update_trading_fees() is not defined, we just exit the loop
        """
        while True:
            try:
                await safe_gather(self._polling_trading_fees())
                await self._sleep(self.TRADING_FEES_INTERVAL)
            except NotImplementedError:
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching trading fees.", exc_info=True,
                    app_warning_msg=f"Could not fetch new trading fees from {self.name_cap}."
                                    " Check network connection.")
                await self._sleep(0.5)

    async def _polling_status_loop(self):
        """
        Performs all required operation to keep the connector updated and synchronized with the exchange.
        It contains the backup logic to update status using API requests in case the main update source
        (the user stream data source websocket) fails.
        It also updates the time synchronizer. This is necessary because the exchange requires
        the time of the client to be the same as the time in the exchange.
        Executes when the _poll_notifier event is enabled by the `tick` function.

        _polling_status_loop
            _polling_status_fetch_updates
                _polling_balances
                _polling_orders
        """
        while True:
            try:
                await self._poll_notifier.wait()
                await self._update_time_synchronizer()

                # the following method is implementation-specific
                await self._polling_status_fetch_updates()

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

    async def _update_time_synchronizer(self):
        try:
            await self._time_synchronizer.update_server_time_offset_with_time_provider(
                time_provider=self.web_utils.get_current_server_time(
                    throttler=self._throttler,
                    domain=self._domain))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error requesting time from {self.name_cap} server")
            raise

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

    # Exchange / Trading logic methods
    # that call the API
    #
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

    async def _api_request(self,
                           path_url,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           limit_id: Optional[str] = None) -> Dict[str, Any]:

        return await self.web_utils.api_request(
            path=path_url,
            api_factory=self._api_factory,
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            params=params,
            data=data,
            method=method,
            is_auth_required=is_auth_required,
            limit_id=limit_id)

    def _get_enqueue_callable(self, channel):
        for ch, clb in self.USER_CHANNELS:
            if channel == ch:
                return clb
        return None

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Traders, Orders, and Balance updates from the WS.
        """
        async for event_message in self._iter_user_event_queue():
            channel: str = event_message.get("channel", None)
            results: str = event_message.get("result", None)
            try:
                enqueue_callable = self._get_enqueue_callable(channel)
                if not enqueue_callable:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.",
                        exc_info=True)
                    continue

                await enqueue_callable(results)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    # Methods called by the Exchange object
    #
    def init_auth(self):
        raise NotImplementedError

    def cancel_order(self):
        raise NotImplementedError

    def create_order(self):
        raise NotImplementedError

    def get_fee(self):
        raise NotImplementedError

    # Internal methods to update data and process
    #
    def _format_trading_rules(self):
        raise NotImplementedError

    # Internal methods for WS messages
    #
    async def _process_balance_message_ws(self):
        raise NotImplementedError

    # Internal methods for polling loops
    #
    async def _polling_status_fetch_updates(self):
        raise NotImplementedError

    async def _polling_orders(self):
        raise NotImplementedError

    async def _polling_balances(self):
        raise NotImplementedError

    async def _polling_trading_fees(self):
        raise NotImplementedError

    async def _polling_trading_rules(self):
        raise NotImplementedError
