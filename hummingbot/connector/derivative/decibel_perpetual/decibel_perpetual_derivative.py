"""
Decibel Perpetual Derivative Connector
Main connector implementation for Decibel perpetual exchange
"""
import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.decibel_perpetual import decibel_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.client.settings import AllConnectorSettings

s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal("0")


class DecibelPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Decibel Perpetual Exchange Connector
    """
    
    web_utils = web_utils
    
    # SHORT_POLL_INTERVAL = 5.0
    # UPDATE_ORDER_STATUS_MODEL = 0.0
    
    def __init__(
        self,
        decibel_perpetual_api_key: str,
        decibel_perpetual_secret_key: str,
        decibel_perpetual_passphrase: Optional[str] = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        """
        Initialize Decibel Perpetual connector
        
        :param decibel_perpetual_api_key: API key for authentication
        :param decibel_perpetual_secret_key: Secret key for signing requests
        :param decibel_perpetual_passphrase: Passphrase (optional)
        :param trading_pairs: List of trading pairs to trade
        :param trading_required: Whether trading is required
        :param domain: Exchange domain
        :param balance_asset_limit: Balance limits for assets
        :param rate_limits_share_pct: Percentage of rate limits to use
        """
        self._decibel_perpetual_api_key = decibel_perpetual_api_key
        self._decibel_perpetual_secret_key = decibel_perpetual_secret_key
        self._decibel_perpetual_passphrase = decibel_perpetual_passphrase
        self._domain = domain
        
        # Initialize throttler
        self._throttler = web_utils.build_rate_limit_throttler()
        
        # Initialize web assistants factory
        self._web_assistants_factory = web_utils.build_api_factory(throttler=self._throttler)
        
        # Initialize authentication
        self._auth = DecibelPerpetualAuth(
            api_key=decibel_perpetual_api_key,
            secret_key=decibel_perpetual_secret_key,
            passphrase=decibel_perpetual_passphrase,
        )
        
        super().__init__(
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
        )
        
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        
    @property
    def authenticator(self) -> DecibelPerpetualAuth:
        """Get authenticator instance"""
        return self._auth
    
    @property
    def rest_assistant(self):
        """Get REST assistant for API requests"""
        return self._web_assistants_factory.rest_assistant
    
    @property
    def ws_assistant(self):
        """Get WebSocket assistant for real-time data"""
        return self._web_assistants_factory.ws_assistant
    
    @property
    def rate_limits(self) -> Dict[str, Any]:
        """Get rate limits"""
        return CONSTANTS.RATE_LIMITS
    
    @property
    def domain(self) -> str:
        """Get exchange domain"""
        return self._domain
    
    @property
    def status_dict(self) -> Dict[str, bool]:
        """Get status dictionary"""
        status = super().status_dict
        status["decibel_perpetual_connected"] = self._throttler is not None
        return status
    
    def supported_position_modes(self) -> List[PositionMode]:
        """Get supported position modes"""
        return [PositionMode.ONEWAY, PositionMode.HEDGE]
    
    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Get collateral token for buy orders"""
        return CONSTANTS.BASE_QUOTE_ASSET
    
    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Get collateral token for sell orders"""
        return CONSTANTS.BASE_QUOTE_ASSET
    
    @property
    def funding_fee_poll_interval(self) -> int:
        """Get funding fee poll interval in seconds"""
        return 60  # Poll every 60 seconds
    
    async def get_all_trading_pairs(self) -> Dict[str, Any]:
        """
        Get all trading pairs from exchange
        
        :return: Dictionary of trading pairs
        """
        try:
            endpoint = "/symbols"
            url = web_utils.build_api_endpoint(endpoint)
            
            response = await self.rest_assistant.execute_request(
                method="GET",
                url=url,
            )
            
            if response.get("success"):
                return response.get("data", {})
            else:
                self.logger().error(f"Failed to get trading pairs: {response}")
                return {}
                
        except Exception as e:
            self.logger().error(f"Error fetching trading pairs: {e}")
            return {}
    
    async def get_trading_rules(self) -> Dict[str, TradingRule]:
        """
        Get trading rules for all trading pairs
        
        :return: Dictionary of trading rules by trading pair
        """
        trading_rules = {}
        
        try:
            all_pairs = await self.get_all_trading_pairs()
            
            for symbol, data in all_pairs.items():
                trading_pair = self._convert_symbol_to_trading_pair(symbol)
                
                trading_rules[trading_pair] = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(str(data.get("minQty", CONSTANTS.MIN_ORDER_SIZE))),
                    max_order_size=Decimal(str(data.get("maxQty", CONSTANTS.MAX_ORDER_SIZE))),
                    min_price_increment=Decimal(str(data.get("tickSize", CONSTANTS.PRICE_TICK_SIZE))),
                    min_base_amount_increment=Decimal(str(data.get("stepSize", CONSTANTS.SIZE_TICK_SIZE))),
                )
            
            return trading_rules
            
        except Exception as e:
            self.logger().error(f"Error fetching trading rules: {e}")
            return {}
    
    def _convert_symbol_to_trading_pair(self, symbol: str) -> str:
        """
        Convert exchange symbol to Hummingbot trading pair format
        
        :param symbol: Exchange symbol (e.g., "BTC-USDT")
        :return: Trading pair (e.g., "BTC-USDT")
        """
        return symbol.replace("-", "-")
    
    def _convert_trading_pair_to_symbol(self, trading_pair: str) -> str:
        """
        Convert Hummingbot trading pair to exchange symbol format
        
        :param trading_pair: Trading pair (e.g., "BTC-USDT")
        :return: Exchange symbol (e.g., "BTC-USDT")
        """
        return trading_pair.replace("-", "-")
    
    async def place_order(
        self,
        order: InFlightOrder,
    ) -> str:
        """
        Place an order on the exchange
        
        :param order: InFlightOrder to place
        :return: Exchange order ID
        """
        try:
            symbol = self._convert_trading_pair_to_symbol(order.trading_pair)
            
            # Build order parameters
            params = {
                "symbol": symbol,
                "side": order.trade_type.name.lower(),
                "type": order.order_type.name.lower(),
                "quantity": str(order.amount),
            }
            
            if order.order_type == OrderType.LIMIT:
                params["price"] = str(order.price)
            
            if order.position_side != PositionSide.FLAT:
                params["positionSide"] = order.position_side.name.lower()
            
            endpoint = "/orders"
            url = web_utils.build_api_endpoint(endpoint)
            
            # Add authentication headers
            headers = self._auth.get_headers("POST", endpoint, str(params))
            
            response = await self.rest_assistant.execute_request(
                method="POST",
                url=url,
                data=params,
                headers=headers,
            )
            
            if response.get("success"):
                order_data = response.get("data", {})
                return order_data.get("orderId")
            else:
                raise Exception(f"Failed to place order: {response}")
                
        except Exception as e:
            self.logger().error(f"Error placing order: {e}")
            raise
    
    async def cancel_order(self, order: InFlightOrder) -> bool:
        """
        Cancel an order
        
        :param order: InFlightOrder to cancel
        :return: True if successful
        """
        try:
            endpoint = f"/orders/{order.exchange_order_id}"
            url = web_utils.build_api_endpoint(endpoint)
            
            headers = self._auth.get_headers("DELETE", endpoint)
            
            response = await self.rest_assistant.execute_request(
                method="DELETE",
                url=url,
                headers=headers,
            )
            
            return response.get("success", False)
            
        except Exception as e:
            self.logger().error(f"Error cancelling order: {e}")
            return False
    
    # Additional methods will be implemented in subsequent iterations
    # Including: order status updates, balance fetching, position management, etc.
