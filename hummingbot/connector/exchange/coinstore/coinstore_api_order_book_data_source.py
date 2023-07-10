import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.coinstore.coinstore_exchange import CoinstoreExchange


class CoinstoreAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "CoinstoreExchange",
        api_factory: Optional[WebAssistantsFactory] = None,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._api_factory = api_factory

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        # params = {"symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair), "depth": 50}
        symbol = params = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=(web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_DEPTH_PATH)).format(symbol),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDERBOOK_DEPTH_PATH,
        )
        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            channels = []
            for trading_pair in self._trading_pairs:
                trading_symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                channels.append(f"{self._connector._symbol_to_id_map[trading_symbol]}@{CONSTANTS.DIFF_EVENT_TYPE}@50")
                channels.append(f"{self._connector._symbol_to_id_map[trading_symbol]}@{CONSTANTS.TRADE_EVENT_TYPE}")
            payload = {
                "op": "SUB",
                "channel": channels,
                "id": 1
            }
            await ws.send(WSJSONRequest(payload=payload))

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = self._time()

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": snapshot_timestamp,
            "bids": snapshot_response["data"]["b"],
            "asks": snapshot_response["data"]["a"],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT, order_book_message_content, snapshot_timestamp
        )

        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        async def parse_message(**msg):
            message_content = {
                "trade_id": msg["tradeId"],
                "trading_pair": await self._connector.trading_pair_associated_to_exchange_symbol(symbol=msg["symbol"]),
                "trade_type": float(TradeType.BUY.value) if msg["takerSide"] == "BUY" else float(TradeType.SELL.value),
                "amount": Decimal(msg["volume"]),
                "price": Decimal(msg["price"]),
            }

            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE, content=message_content, timestamp=msg["time"] / 1000
            )
            message_queue.put_nowait(trade_message)

        for trade_data in raw_message["data"]:
            await parse_message(**trade_data)

        # parse incremental update
        if raw_message.get("tradeId", None) is not None:
            await parse_message(**raw_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_data: Dict[str, Any] = raw_message
        timestamp: float = self._time()

        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["symbol"])

        message_content = {
            "trading_pair": trading_pair,
            "update_id": timestamp,
            "bids": diff_data["b"],
            "asks": diff_data["a"],
        }
        diff_message: OrderBookMessage = OrderBookMessage(OrderBookMessageType.DIFF, message_content, timestamp)

        message_queue.put_nowait(diff_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if event_message.get("T") == CONSTANTS.TRADE_EVENT_TYPE or event_message.get("tradeId") is not None:
            channel = self._trade_messages_queue_key
        if event_message.get("T") == CONSTANTS.DIFF_EVENT_TYPE:
            channel = self._diff_messages_queue_key
        return channel

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ):
        # Send pong whenever an unidentified message is received since client is allowed to send several pong frames
        pong_payloads = {"op": "pong", "epochMillis": self._time() * 1e3}
        pong_request = WSJSONRequest(payload=pong_payloads)
        await websocket_assistant.send(request=pong_request)
