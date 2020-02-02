import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.position_sizer.weight import WeightPositionSizer 


class BuyAndHoldStopLossStrategy(AbstractStrategy):
    """
    A testing strategy that simply purchases (longs) an asset
    upon first receipt of the relevant bar event and
    then holds until the completion of a backtest.
    """
    def __init__(
        self, tickers, 
        events_queue, 
        base_quantity=100
    ):
        """
        TODO
        """
        self.tickers = tickers
        self.events_queue = events_queue
        self.tickers_invested = self._create_invested_list()
        self.maximum_prices = self._create_maximum_list()

    def _create_invested_list(self):
        """
        Create a dictionary with each ticker as a key, with
        a boolean value depending upon whether the ticker has
        been "invested" yet. This is necessary to avoid sending
        a liquidation signal on the first allocation.
        """
        tickers_invested = {ticker: False for ticker in self.tickers}

        return tickers_invested

    def _create_maximum_list(self):
        """
        TODO
        """
        maximum_prices = {ticker: 0 for ticker in self.tickers}

        return maximum_prices
 
    def calculate_signals(self, event):
        if (
            event.type in [EventType.BAR, EventType.TICK]
            ):
            
            ticker = event.ticker
            # Update historical maximum price for the ticker
            if self.maximum_prices[ticker] < event.high_price:
                self.maximum_prices[ticker] = event.high_price

            # Initial Investment
            if self.tickers_invested[ticker] == False and event.high_price == self.maximum_prices[ticker]:
                signal = SignalEvent(
                    ticker, "BOT",
                )
                self.events_queue.put(signal)
                self.tickers_invested[ticker] = True
          
            # Stop Loss Signal
            if self.tickers_invested[ticker] == True and event.low_price <= self.maximum_prices[ticker] * (1 - 0.1):
                signal = SignalEvent(
                    ticker, "STOP_LOSS",
                )
                self.events_queue.put(signal)
                self.tickers_invested[ticker] = False

def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Buy and Hold Example on %s' % tickers[:]]
    initial_equity = 10000.0
    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime(2020, 1, 1)
    ticker_weights = {
        "SPY": 0.2,
        "AAPL":0.2,
        "AGG":0.2,
        "KEYS":0.2,
    }

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldStopLossStrategy(tickers, events_queue)

    # Position Sizer
    position_sizer = WeightPositionSizer(ticker_weights)

    # Set up the backtest
    backtest = TradingSession(
        config, strategy, tickers,
        initial_equity, start_date, end_date,
        events_queue, position_sizer=position_sizer,
        title=title, benchmark=tickers[0]
    )
    results = backtest.start_trading(testing=testing)
    return results


if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )
    tickers = ["SPY", "AAPL", "AGG", "KEYS"]
    filename = None
    run(config, testing, tickers, filename)
