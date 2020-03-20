import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.signal_sizer.fixed import FixedSignalSizer


class BuyAndHoldStrategy(AbstractStrategy):
    """
    A testing strategy that simply purchases (longs) an asset
    upon first receipt of the relevant bar event and
    then holds until the completion of a backtest.
    """
    def __init__(
        self, ticker, 
        events_queue,
    ):
        self.ticker = ticker
        self.events_queue = events_queue
        self.invested = False

    def calculate_signals(self, event, portfolio_handler):
        if (
            event.type in [EventType.BAR, EventType.TICK] and
            event.ticker == self.ticker
        ):
            if not self.invested:
    
                signal = SignalEvent(
                    self.ticker, "BOT",
                )
                # Select type of sizer and size the quantity of the signal
                signal_sizer = FixedSignalSizer()
                sized_signal = signal_sizer.size_signal(
                    portfolio_handler.portfolio, signal
                )
                self.events_queue.put(sized_signal)
                self.invested = True

def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Buy and Hold Example on %s' % tickers[0]]
    initial_equity = 10000.0
    start_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2020, 3, 19)

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldStrategy(tickers[0], events_queue)

    # Set up the backtest
    backtest = TradingSession(
        config, strategy, tickers,
        initial_equity, start_date, end_date,
        events_queue, title=title, benchmark=tickers[1]
    )
    results = backtest.start_trading(testing=testing)
    return results


if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )
    tickers = ["KEYS", "SPY"]
    filename = None
    run(config, testing, tickers, filename)