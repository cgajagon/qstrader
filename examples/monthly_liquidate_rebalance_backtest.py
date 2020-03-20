import calendar
import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.signal_sizer.rebalance import LiquidateRebalanceSignalSizer
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession


class MonthlyLiquidateRebalanceStrategy(AbstractStrategy):
    """
    A generic strategy that allows monthly rebalancing of a
    set of tickers, via full liquidation and dollar-weighting
    of new positions.
    """
    def __init__(self, tickers, events_queue, ticker_weights):
        self.tickers = tickers
        self.events_queue = events_queue
        self.ticker_weights = ticker_weights
        self.tickers_invested = self._create_invested_list()

    def _end_of_month(self, cur_time):
        """
        Determine if the current day is at the end of the month.
        """
        cur_day = cur_time.day
        end_day = calendar.monthrange(cur_time.year, cur_time.month)[1]
        return cur_day == end_day

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
        """
        For a particular received BarEvent, determine whether
        it is the end of the month (for that bar) and generate
        a liquidation signal, as well as a purchase signal,
        for each ticker.
        """
        if (
            event.type in [EventType.BAR, EventType.TICK] and
            self._end_of_month(event.time)
        ):
            ticker = event.ticker
            if self.tickers_invested[ticker]: # Exit position
                liquidate_signal = SignalEvent(ticker, "EXIT")
                liquidate_signal.suggested_quantity = portfolio_handler.portfolio.positions[ticker].quantity
                self.events_queue.put(liquidate_signal)
            # Enter position     
            long_signal = SignalEvent(ticker, "BOT")
            # Select type of sizer and size the quantity of the signal
            signal_sizer = LiquidateRebalanceSignalSizer(self.ticker_weights)
            sized_signal = signal_sizer.size_signal(
                portfolio_handler.portfolio, long_signal
            )
            self.events_queue.put(sized_signal)
            self.tickers_invested[ticker] = True


def run(config, testing, tickers, filename):
    # Backtest information
    title = [
        'Monthly Liquidate/Rebalance on 60%/40% SPY/AGG Portfolio'
    ]
    initial_equity = 10000.0
    start_date = datetime.datetime(2018, 1, 1)
    end_date = datetime.datetime(2020, 1, 1)
    ticker_weights = {
        "SPY": 0.5,
        "AAPL":0.5,
    }

    # Use the Monthly Liquidate And Rebalance strategy
    events_queue = queue.Queue()
    strategy = MonthlyLiquidateRebalanceStrategy(
        tickers, events_queue, ticker_weights
    )

    # Set up the backtest
    backtest = TradingSession(
        config, strategy, tickers,
        initial_equity, start_date, end_date,
        events_queue, title=title, benchmark=tickers[0],
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
