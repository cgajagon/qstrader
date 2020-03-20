import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.signal_sizer.weight import WeightSignalSizer 


class BuyAndHoldMultipleStrategy(AbstractStrategy):
    """
    A testing strategy that simply purchases (longs) an asset
    upon first receipt of the relevant bar event and
    then holds until the completion of a backtest.
    """
    def __init__(
        self, tickers, 
        events_queue, 
        ticker_weights,
    ):
        self.tickers = tickers
        self.events_queue = events_queue
        self.ticker_weights = ticker_weights
        self.tickers_invested = self._create_invested_list()

    def _create_invested_list(self):
        """
        Create a dictionary with each ticker as a key, with
        a boolean value depending upon whether the ticker has
        been "invested" yet. This is necessary to avoid sending
        a liquidation signal on the first allocation.
        """
        tickers_invested = {ticker: False for ticker in self.tickers}
        return tickers_invested
        
    def calculate_signals(self, event, portfolio_handler):
        if (
            event.type in [EventType.BAR, EventType.TICK]
            ):
            
            ticker = event.ticker
            if not self.tickers_invested[ticker]:

                signal = SignalEvent(
                    ticker, "BOT",
                )
                # Select type of sizer and size the quantity of the signal
                signal_sizer = WeightSignalSizer(self.ticker_weights)
                sized_signal = signal_sizer.size_signal(
                    portfolio_handler.portfolio, signal
                )
                self.events_queue.put(sized_signal)
                self.tickers_invested[ticker] = True

def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Buy and Hold Example on %s' % tickers[:]]
    initial_equity = 10000.0
    start_date = datetime.datetime(2012, 1, 1)
    end_date = datetime.datetime(2020, 3, 1)
    ticker_weights = {
        "SPY":0.5,
        "AAPL": 0.5,
    }

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldMultipleStrategy(
        tickers, events_queue, ticker_weights
        )

    # Set up the backtest
    backtest = TradingSession(
        config, strategy, tickers,
        initial_equity, start_date, end_date,
        events_queue, title=title, benchmark=tickers[0]
    )
    results = backtest.start_trading(testing=testing)
    return results

if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )
    tickers = ["SPY", "AAPL"]
    filename = None
    run(config, testing, tickers, filename)
