import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.position_sizer.fixed import FixedPositionSizer


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

    def calculate_signals(self, event):
        if (
            event.type in [EventType.BAR, EventType.TICK] and
            event.ticker == self.ticker
        ):
            if not self.invested:
                signal = SignalEvent(
                    self.ticker, "BOT",
                )
                self.events_queue.put(signal)
                self.invested = True

def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Buy and Hold Example on %s' % tickers[0]]
    initial_equity = 10000.0
    start_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2020, 1, 1)

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldStrategy(tickers[0], events_queue)

    position_sizer = FixedPositionSizer()

    # Set up the backtest
    backtest = TradingSession(
        config, strategy, tickers,
        initial_equity, start_date, end_date,
        events_queue, position_sizer=position_sizer,
        title=title, benchmark=tickers[1]
    )
    results = backtest.start_trading(testing=testing)
    return results


if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )
    tickers = ["V", "SPY"]
    filename = None
    run(config, testing, tickers, filename)