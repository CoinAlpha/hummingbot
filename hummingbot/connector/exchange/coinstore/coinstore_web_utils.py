from typing import Optional

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CoinstoreRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.headers is None:
            request.headers = {}
        # Generates generic headers required
        headers_generic = {}
        headers_generic["Accept"] = "application/json"
        headers_generic["Content-Type"] = "application/json"
        # Headers signature to identify user as an HB liquidity provider.
        request.headers = dict(list(request.headers.items()) + list(headers_generic.items()))
        return request


def public_rest_url(path_url: str, domain: Optional[str] = None) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: domain to connect to
    :return: the full URL to the endpoint
    """
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: Optional[str] = None) -> str:
    return public_rest_url(path_url=path_url)


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler, auth=auth, rest_pre_processors=[CoinstoreRESTPreProcessor()]
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)
