from math import floor

from .base import AbstractSignalSizer
from qstrader.price_parser import PriceParser


class LiquidateRebalanceSignalSizer(AbstractSignalSizer):
    """
    Carries out a periodic full liquidation and rebalance of
    the Portfolio.

    This is achieved by determining whether an order type type
    is "EXIT" or "BOT/SLD".

    If the former, the current quantity of shares in the ticker
    is determined and then BOT or SLD to net the position to zero.

    If the latter, the current quantity of shares to obtain is
    determined by prespecified weights and adjusted to reflect
    current account equity.
    """
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_signal(self, portfolio, signal):
        """
        Size the signal to reflect the dollar-weighting of the
        current equity account size based on pre-specified
        ticker weights.
        """
        ticker = signal.ticker
        
        if signal.action == "EXIT":
            # Obtain current quantity and liquidate
            cur_quantity = portfolio.positions[ticker].quantity
            if cur_quantity > 0:
                signal.action = "SLD"
                signal.quantity = cur_quantity
            else:
                signal.action = "BOT"
                signal.suggested_quantity = cur_quantity
        else:
            weight = self.ticker_weights[ticker]
            # Determine total portfolio value, work out dollar weight
            # and finally determine integer quantity of shares to purchase
            price = portfolio.price_handler.tickers[ticker]["close"]
            price = PriceParser.display(price)
            equity = PriceParser.display(portfolio.equity)
            dollar_weight = weight * equity
            weighted_quantity = int(floor(dollar_weight / price))
            signal.suggested_quantity = weighted_quantity
        return signal

class RebalanceSignalSizer(AbstractSignalSizer):
    """
    Carries out a periodic full liquidation and rebalance of
    the Portfolio.

    This is achieved by determining whether an order type type
    is "EXIT" or "BOT/SLD".

    If the former, the current quantity of shares in the ticker
    is determined and then BOT or SLD to net the position to zero.

    If the latter, the current quantity of shares to obtain is
    determined by prespecified weights and adjusted to reflect
    current account equity.
    """
    def __init__(self, ticker_weights):
        self.ticker_weights = ticker_weights

    def size_signal(self, portfolio, signal):
        """
        Size the signal to reflect the dollar-weighting of the
        current equity account size based on pre-specified
        ticker weights.
        """
        ticker = signal.ticker
        weight = self.ticker_weights[ticker]
        # Determine total portfolio value, work out dollar weight
        # and finally determine integer quantity of shares to purchase
        price = portfolio.price_handler.tickers[ticker]["close"]
        price = PriceParser.display(price)
        equity = PriceParser.display(portfolio.equity)
        dollar_weight = weight * equity
        weighted_quantity = int(floor(dollar_weight / price))

        if signal.action == "REBALANCE":
            # Obtain current quantity
            cur_quantity = portfolio.positions[ticker].quantity
            if cur_quantity - weighted_quantity > 0:
                signal.action = "SLD"
                signal.suggested_quantity = cur_quantity - weighted_quantity
            else:
                signal.action = "BOT"
                signal.suggested_quantity = weighted_quantity - cur_quantity
        else:
            signal.suggested_quantity = weighted_quantity

        return signal
