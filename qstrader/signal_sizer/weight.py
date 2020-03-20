from math import floor

from .base import AbstractSignalSizer
from qstrader.price_parser import PriceParser


class WeightSignalSizer(AbstractSignalSizer):
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_signal(self, portfolio, signal):
        """
        This WeightSignalSizer object divides the cash available 
        among the tickers selected.
        """
        ticker = signal.ticker
        weight = dict(self.ticker_weights)

        if signal.action == 'BOT':
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
            signal.suggested_quantity = int(floor(weight_quantity))
      
        elif signal.action == 'STOP_LOSS':
            # Liquidate the position
            cur_quantity = portfolio.positions[ticker].quantity
            signal.suggested_quantity = cur_quantity
            signal.action = 'SLD'

        return signal

class WeightComplexSignalSizer(AbstractSignalSizer):
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_signal(self, portfolio, signal):
        """
        This WeightComplexSignalSizer object divides the cash available 
        among the tickers selected.
        """
        ticker = signal.ticker
        weight = self.ticker_weights[ticker]
        # Determine total portfolio value, work out dollar weight
        # and finally determine integer quantity of shares to purchase
        price = PriceParser.display(
            portfolio.price_handler.tickers[ticker]["close"]
            )
        equity = PriceParser.display(portfolio.equity)
        cur_cash = PriceParser.display(portfolio.cur_cash)
        dollar_weight = weight * equity
        weighted_quantity = int(floor(dollar_weight / price))
        allowed_quantity = int(floor(cur_cash / price))
        
        if signal.action == "BOT": # Enter the position
            if allowed_quantity >= weighted_quantity: # Enough cash available
                signal.suggested_quantity = weighted_quantity
            else: # Not enough cash available
                signal.suggested_quantity = allowed_quantity

        else:
            cur_quantity = portfolio.positions[ticker].quantity
            if signal.action == "REBALANCE": # Rebalance the position
                if cur_quantity - weighted_quantity > 0: # Reduce the position quantity
                    signal.action = "SLD"
                    signal.suggested_quantity = cur_quantity - weighted_quantity
                else: # Increase the position quantity
                    signal.action = "BOT"
                    if allowed_quantity >= weighted_quantity - cur_quantity: # Enough cash available
                        signal.suggested_quantity = weighted_quantity - cur_quantity
                    else: # Not enough cash available
                        signal.suggested_quantity = allowed_quantity
            elif signal.action == 'STOP_LOSS': # Liquidate the position
                signal.suggested_quantity = cur_quantity
                signal.action = 'SLD'

        return signal