import time
from decimal import Decimal
from typing import List

from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order import Order
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

CONNECTOR = "injective_injective_mainnet"
TRADING_PAIR = combine_to_hb_trading_pair(base="ATOM", quote="USDT")
AMOUNT = Decimal("0.15")
ORDERS_INTERVAL = 20
PRICE_OFFSET_RATIO = Decimal("0.1")  # 10%


class BatchOrderUpdate(ScriptStrategyBase):
    markets = {CONNECTOR: {TRADING_PAIR}}
    pingpong = 0

    orders_placed_ts = time.time() - ORDERS_INTERVAL

    def on_tick(self):
        if time.time() - self.orders_placed_ts > ORDERS_INTERVAL:
            self.refresh_orders()
            self.orders_placed_ts = time.time()

    def refresh_orders(self):
        price = self.connectors[CONNECTOR].get_price(trading_pair=TRADING_PAIR, is_buy=True)
        orders_to_create = [
            Order(
                trading_pair=TRADING_PAIR,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=AMOUNT,
                price=price * (1 - PRICE_OFFSET_RATIO),
            ),
            Order(
                trading_pair=TRADING_PAIR,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.SELL,
                amount=AMOUNT,
                price=price * (1 + PRICE_OFFSET_RATIO),
            )
        ]
        orders_to_cancel = [
            Order(
                trading_pair=order.trading_pair,
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY if order.is_buy else TradeType.SELL,
                amount=order.quantity,
                price=order.price,
                client_order_id=order.client_order_id,
            ) for order in self.get_active_orders(connector_name=CONNECTOR)
        ]

        market_pair = self._market_trading_pair_tuple(connector_name=CONNECTOR, trading_pair=TRADING_PAIR)
        market = market_pair.market

        submitted_orders: List[Order] = market.batch_order_update(
            orders_to_create=orders_to_create,
            orders_to_cancel=orders_to_cancel,
        )

        for order in submitted_orders:
            self.start_tracking_limit_order(
                market_pair=market_pair,
                order_id=order.client_order_id,
                is_buy=order.trade_type == TradeType.BUY,
                price=order.price,
                quantity=order.amount,
            )
