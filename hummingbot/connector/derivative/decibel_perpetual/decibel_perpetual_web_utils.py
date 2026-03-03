"""
Decibel Perpetual Web Utilities
REST and WebSocket utilities for API communication
"""
import asyncio
from typing import Any, Dict, Optional, Union

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from async_timeout import timeout

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.connections.data_types import RESTConnection, RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


class DecibelPerpetualWebUtils:
    """
    Web utilities for Decibel Perpetual API
    """
    
    @staticmethod
    def build_api_factory(throttler: Optional[AsyncThrottler] = None) -> WebAssistantsFactory:
        """
        Build WebAssistantsFactory for REST and WebSocket communication
        
        :param throttler: AsyncThrottler instance for rate limiting
        :return: WebAssistantsFactory instance
        """
        rest_connection = RESTConnection(
            url=CONSTANTS.BASE_REST_URL,
            timeout=ClientTimeout(total=CONSTANTS.MESSAGE_TIMEOUT),
        )
        
        ws_connection = WSJSONRequest(
            url=CONSTANTS.BASE_WS_URL,
        )
        
        return WebAssistantsFactory(
            rest_assistant=RESTAssistant(
                connection=rest_connection,
                throttler=throttler,
            ),
            ws_assistant=WSAssistant(
                connection=ws_connection,
                throttler=throttler,
            ),
        )
    
    @staticmethod
    def build_api_endpoint(path: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
        """
        Build full API endpoint URL
        
        :param path: API endpoint path
        :param domain: Domain (default, testnet, etc.)
        :return: Full URL
        """
        return f"{CONSTANTS.BASE_REST_URL}/{CONSTANTS.API_VERSION}{path}"
    
    @staticmethod
    def build_ws_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
        """
        Build WebSocket URL
        
        :param domain: Domain (default, testnet, etc.)
        :return: WebSocket URL
        """
        return CONSTANTS.BASE_WS_URL
    
    @staticmethod
    async def get_rest_assistant(throttler: Optional[AsyncThrottler] = None) -> RESTAssistant:
        """
        Get REST assistant for API requests
        
        :param throttler: AsyncThrottler for rate limiting
        :return: RESTAssistant instance
        """
        factory = DecibelPerpetualWebUtils.build_api_factory(throttler)
        return factory.rest_assistant
    
    @staticmethod
    async def get_ws_assistant(throttler: Optional[AsyncThrottler] = None) -> WSAssistant:
        """
        Get WebSocket assistant for real-time data
        
        :param throttler: AsyncThrottler for rate limiting
        :return: WSAssistant instance
        """
        factory = DecibelPerpetualWebUtils.build_api_factory(throttler)
        return factory.ws_assistant
    
    @staticmethod
    def build_rate_limit_throttler() -> AsyncThrottler:
        """
        Build rate limit throttler based on exchange limits
        
        :return: AsyncThrottler instance
        """
        return AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
    
    @staticmethod
    def is_public_endpoint(endpoint: str) -> bool:
        """
        Check if endpoint is public (doesn't require authentication)
        
        :param endpoint: API endpoint path
        :return: True if public, False if private
        """
        public_endpoints = [
            "/ticker",
            "/orderbook",
            "/trades",
            "/klines",
            "/symbols",
            "/server/time",
        ]
        
        return any(endpoint.startswith(path) for path in public_endpoints)
    
    @staticmethod
    def parse_error_response(error: Dict[str, Any]) -> str:
        """
        Parse error response from API
        
        :param error: Error response dictionary
        :return: Formatted error message
        """
        if "message" in error:
            return error["message"]
        elif "msg" in error:
            return error["msg"]
        elif "error" in error:
            return str(error["error"])
        else:
            return "Unknown error"
    
    @staticmethod
    def prepare_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Prepare request parameters (remove None values)
        
        :param params: Request parameters
        :return: Cleaned parameters dictionary
        """
        if params is None:
            return {}
        
        return {k: v for k, v in params.items() if v is not None}
