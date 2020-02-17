from math import floor
from .base import AbstractPositionSizer


class FixedPositionSizer(AbstractPositionSizer):
    def __init__(self, default_quantity=100):
        self.default_quantity = default_quantity

    def size_order(self, portfolio, initial_order):
        """
        This FixedPositionSizer object simply modifies
        the quantity to be a default quantity of any share transacted.
        """

        ticker = initial_order.ticker

        init_cash = portfolio.init_cash
        price = portfolio.price_handler.tickers[ticker]["close"]
        quantity = int(floor(init_cash/price))
        
        initial_order.quantity = quantity

        return initial_order
