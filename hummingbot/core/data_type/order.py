from _decimal import Decimal
from dataclasses import dataclass, field
from typing import Dict, Optional

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder


@dataclass
class Order:
    trading_pair: str
    order_type: OrderType
    trade_type: TradeType
    amount: Decimal
    price: Decimal
    client_order_id: Optional[str] = None
    kwargs: Dict[str, any] = field(default_factory=lambda: {})

    @classmethod
    def from_in_flight_order(cls, in_flight_order: InFlightOrder) -> "Order":
        return Order(
            trading_pair=in_flight_order.trading_pair,
            order_type=in_flight_order.order_type,
            trade_type=in_flight_order.trade_type,
            amount=in_flight_order.amount,
            price=in_flight_order.price,
            client_order_id=in_flight_order.client_order_id,
        )
