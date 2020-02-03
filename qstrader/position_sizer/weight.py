from math import floor

from .base import AbstractPositionSizer
from qstrader.price_parser import PriceParser


class WeightPositionSizer(AbstractPositionSizer):
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_order(self, portfolio, initial_order):
        """
        This WeightPositionSizer object divides the cash available 
        among the tickers selected.
        """
        ticker = initial_order.ticker
        weight = dict(self.ticker_weights)

        if initial_order.action == 'BOT':
            # Determine current cash available in the portfolio, work out dollar weight
            # and finally determine integer quantity of shares to purchase
            price = portfolio.price_handler.tickers[ticker]["close"]
            price = PriceParser.display(price)
            cur_cash = PriceParser.display(portfolio.cur_cash)
            position_list = portfolio.positions
            for key, values in position_list.items():
                weight[key] = 0

            # Calculate the total weight not yet allocated yet
            remaining_weight = sum(weight.values())

            # Adjust the weight for the ticker not yet allocated 
            for key, values in weight.items():
                if weight[key] != 0:
                    weight[key] =  weight[key] / remaining_weight

            # Determine the integer quantity of shares
            allocated_cash = cur_cash * weight[ticker]
            weight_quantity = (allocated_cash / price)
            initial_order.quantity = int(floor(weight_quantity))
      
        elif initial_order.action == 'STOP_LOSS':
            # Liquidate the position
            cur_quantity = portfolio.positions[ticker].quantity
            initial_order.quantity = cur_quantity
            initial_order.action = 'SLD'

        return initial_order