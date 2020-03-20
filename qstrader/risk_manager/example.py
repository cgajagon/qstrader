from .base import AbstractRiskManager
from ..event import OrderEvent


class ExampleRiskManager(AbstractRiskManager):
    def refine_orders(self, portfolio, signal_event):
        """
        This ExampleRiskManager object simply lets the
        sized order through, creates the corresponding
        OrderEvent object and adds it to a list.
        """
        # Eliminate Null quantity orders and convert it into OrderEvent
        if signal_event.suggested_quantity > 0:
            order_event = OrderEvent(
                signal_event.ticker,
                signal_event.action,
                signal_event.suggested_quantity
            )
        else:
            order_event = None
        return order_event
