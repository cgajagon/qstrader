from math import floor

from .base import AbstractPositionSizer
from qstrader.price_parser import PriceParser


class WeightPositionSizer(AbstractPositionSizer):
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_order(self, portfolio, initial_order):
        """
        This FixedPositionSizer object simply modifies
        the quantity to be a default quantity of any share transacted.
        """
        ticker = initial_order.ticker
        weight = self.ticker_weights

        if initial_order.action == 'BOT':
            # Determine total portfolio value, work out dollar weight
            # and finally determine integer quantity of shares to purchase
            price = portfolio.price_handler.tickers[ticker]["adj_close"]
            price = PriceParser.display(price)
            cur_cash = PriceParser.display(portfolio.cur_cash)
            positions_dict = portfolio.positions

            for key, value in positions_dict.items():
                print('Key is:',key)
                weight[key] = 0
            print('Updated: ',weight)

            allocated_cash = cur_cash * self.ticker_weights[ticker]
            weight_quantity = (allocated_cash / price)
            initial_order.quantity = int(floor(weight_quantity))

       
        elif initial_order.action == 'STOP_LOSS':
            cur_quantity = portfolio.positions[ticker].quantity
            initial_order.quantity = cur_quantity
            initial_order.action == 'SLD'

        return initial_order