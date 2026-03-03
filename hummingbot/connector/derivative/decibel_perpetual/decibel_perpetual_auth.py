"""
Decibel Perpetual Authentication
Handles API key authentication and request signing
"""
import base64
import hmac
import hashlib
import time
from typing import Dict, Optional

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS


class DecibelPerpetualAuth:
    """
    Authentication class for Decibel Perpetual API
    """
    
    def __init__(self, api_key: str, secret_key: str, passphrase: Optional[str] = None):
        """
        Initialize authentication credentials
        
        :param api_key: API key for authentication
        :param secret_key: Secret key for signing requests
        :param passphrase: Passphrase (if required by exchange)
        """
        self._api_key: str = api_key
        self._secret_key: str = secret_key
        self._passphrase: Optional[str] = passphrase
    
    def get_headers(self, method: str, path: str, body: str = "", timestamp: Optional[int] = None) -> Dict[str, str]:
        """
        Generate authentication headers for API request
        
        :param method: HTTP method (GET, POST, DELETE, etc.)
        :param path: API endpoint path
        :param body: Request body (empty string for GET requests)
        :param timestamp: Request timestamp (uses current time if not provided)
        :return: Dictionary of authentication headers
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        
        # Create signature message
        message = self._generate_signature_message(method, path, body, timestamp)
        
        # Sign message
        signature = self._sign(message)
        
        # Build headers
        headers = {
            "X-API-KEY": self._api_key,
            "X-TIMESTAMP": str(timestamp),
            "X-SIGNATURE": signature,
            "Content-Type": "application/json",
        }
        
        if self._passphrase is not None:
            headers["X-PASSPHRASE"] = self._passphrase
        
        return headers
    
    def _generate_signature_message(self, method: str, path: str, body: str, timestamp: int) -> str:
        """
        Generate the message to be signed
        
        :param method: HTTP method
        :param path: API path
        :param body: Request body
        :param timestamp: Request timestamp
        :return: Formatted signature message
        """
        # Format: timestamp + method + path + body
        return f"{timestamp}{method}{path}{body}"
    
    def _sign(self, message: str) -> str:
        """
        Sign the message using HMAC-SHA256
        
        :param message: Message to sign
        :return: Hex-encoded signature
        """
        mac = hmac.new(
            self._secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256
        )
        return mac.hexdigest()
    
    def get_ws_auth_payload(self) -> Dict[str, str]:
        """
        Generate authentication payload for WebSocket connection
        
        :return: Dictionary with authentication parameters
        """
        timestamp = int(time.time() * 1000)
        message = f"authentication{timestamp}"
        signature = self._sign(message)
        
        return {
            "apiKey": self._api_key,
            "timestamp": str(timestamp),
            "signature": signature,
        }
    
    @staticmethod
    def generate_ws_auth_message(api_key: str, secret_key: str, timestamp: int) -> Dict[str, str]:
        """
        Static method to generate WebSocket authentication message
        
        :param api_key: API key
        :param secret_key: Secret key
        :param timestamp: Current timestamp
        :return: Authentication payload
        """
        message = f"authentication{timestamp}"
        mac = hmac.new(
            secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256
        )
        
        return {
            "type": "auth",
            "apiKey": api_key,
            "timestamp": str(timestamp),
            "signature": mac.hexdigest(),
        }
