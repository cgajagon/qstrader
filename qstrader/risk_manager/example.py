from .base import AbstractRiskManager
from ..event import OrderEvent


class ExampleRiskManager(AbstractRiskManager):
    def refine_orders(self, portfolio, sized_order):
        """
        This ExampleRiskManager object simply lets the
        sized order through, creates the corresponding
        OrderEvent object and adds it to a list.
        """
        # Eliminate Null quantity orders
        if sized_order.quantity > 0:
            order_event = OrderEvent(
                sized_order.ticker,
                sized_order.action,
                sized_order.quantity
            )
        else:
            order_event = None
        return [order_event]
