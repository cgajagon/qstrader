import datetime

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.signal_sizer.weight import WeightSignalSizer 


class BuyAndHoldStopLossStrategy(AbstractStrategy):
    """
    A testing strategy that purchases (longs) an asset
    upon first receipt of the maximum price up to date and
    then holds until either the completion of a backtest
    or the value of the position drops below a stop loss price.
    """
    def __init__(
        self, tickers, 
        events_queue, 
        risk_pct,
        ticker_weights,
    ):
        """
        TODO
        """
        self.tickers = tickers
        self.events_queue = events_queue
        self.risk_pct = risk_pct
        self.ticker_weights = ticker_weights
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
 
    def calculate_signals(self, event, portfolio_handler):
        if (
            event.type in [EventType.BAR, EventType.TICK]
            ):
            
            ticker = event.ticker
            signal_sizer = WeightSignalSizer(self.ticker_weights)
            # Update historical maximum price for the ticker
            if self.maximum_prices[ticker] < event.high_price:
                self.maximum_prices[ticker] = event.high_price

            # Long Signal when the price is the maximum
            if self.tickers_invested[ticker] == False and event.high_price == self.maximum_prices[ticker]:
                signal = SignalEvent(
                    ticker, "BOT",
                )
                # Select type of sizer and size the quantity of the signal
                sized_signal = signal_sizer.size_signal(
                    portfolio_handler.portfolio, signal
                )
                self.events_queue.put(sized_signal)
                self.tickers_invested[ticker] = True
          
            # Stop Loss Signal
            if self.tickers_invested[ticker] == True and event.low_price <= self.maximum_prices[ticker] * (1 - self.risk_pct):
                signal = SignalEvent(
                    ticker, "STOP_LOSS",
                )
                # Select type of sizer and size the quantity of the signal
                sized_signal = signal_sizer.size_signal(
                    portfolio_handler.portfolio, signal
                )
                self.events_queue.put(sized_signal)
                self.tickers_invested[ticker] = False

def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Buy and Hold Example on %s' % tickers[:]]
    initial_equity = 10000.0
    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime(2020, 1, 1)
    ticker_weights = {
        "SPY": 0.5,
        "AAPL":0.5,
    }
    
    # Acceptable drop percentage for the position
    risk_pct = 0.2

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldStopLossStrategy(
        tickers, events_queue, risk_pct, ticker_weights
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
