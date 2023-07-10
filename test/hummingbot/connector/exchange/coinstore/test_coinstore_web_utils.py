from unittest import TestCase

from hummingbot.connector.exchange.coinstore import coinstore_constants as CONSTANTS, coinstore_web_utils as web_utils


class WebUtilsTests(TestCase):
    def test_rest_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH)
        self.assertEqual('https://api.coinstore.com/api/v1/market/trade/{}', url)
        url = web_utils.private_rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH)
        self.assertEqual('https://api.coinstore.com/api/v1/market/trade/{}', url)
