from math import floor
from .base import AbstractSignalSizer


class FixedSignalSizer(AbstractSignalSizer):
    def __init__(self, default_quantity=100):
        self.default_quantity = default_quantity

    def size_signal(self, portfolio, signal):
        """
        This FixedPositionSizer object simply modifies
        the quantity to be a default quantity of any share transacted.
        """

        ticker = signal.ticker

        init_cash = portfolio.init_cash
        price = portfolio.price_handler.tickers[ticker]["close"]
        quantity = int(floor(init_cash/price))
        
        signal.suggested_quantity = quantity

        return signal
