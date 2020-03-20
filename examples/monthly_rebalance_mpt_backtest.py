import datetime
import calendar

from pypfopt import risk_models, expected_returns
from pypfopt.efficient_frontier import EfficientFrontier

import pandas as pd

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession
from qstrader.signal_sizer.weight import WeightComplexSignalSizer
from qstrader.price_parser import PriceParser


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
    ):
        """
        TODO
        """
        self.tickers = tickers
        self.events_queue = events_queue
        self.risk_pct = risk_pct
        self.tickers_invested = self._create_invested_list()
        self.maximum_prices = self._create_maximum_list()
        self.df_close_price = self._create_event_list()
        
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

    def _create_event_list(self):
        """
        TODO
        """
        df_close_price = pd.DataFrame(columns=["date","close_price","ticker"])
        return df_close_price

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

            # Update historical maximum price for the ticker
            if self.maximum_prices[ticker] < event.high_price:
                self.maximum_prices[ticker] = event.high_price
            # Store close price
            data = {'date':[event.time.strftime("%Y-%m-%d")], 'close_price':[PriceParser.display(event.close_price)], 'ticker':[ticker]}
            df = pd.DataFrame(data)
            self.df_close_price = self.df_close_price.append(df)
            # Update investment list
            if ticker in portfolio_handler.portfolio.positions:
                self.tickers_invested[ticker] = True
            else:
                self.tickers_invested[ticker] = False 
            # Stop Loss Signal
            if self.tickers_invested[ticker] and event.low_price <= self.maximum_prices[ticker] * (1 - self.risk_pct):
                signal = SignalEvent(
                    ticker, "SLD",
                )
                # Size the quantity of the signal
                signal.suggested_quantity = portfolio_handler.portfolio.positions[ticker].quantity
                self.events_queue.put(signal) 
            # Enter or Rebalance Signal
            elif self._end_of_month(event.time) and event.low_price > self.maximum_prices[ticker] * (1 - self.risk_pct):
                # Calculate expected returns and sample covariance
                clean = self.df_close_price.set_index('date')
                table = clean.pivot(columns='ticker', values='close_price')
                mu = expected_returns.ema_historical_return(table, span=500)
                S = risk_models.exp_cov(table, span=180)
                # Optimise for maximal Sharpe ratio
                ef = EfficientFrontier(mu, S)
                raw_weights = ef.max_sharpe()
                ticker_weights = ef.clean_weights()
                # Select type of sizer
                signal_sizer = WeightComplexSignalSizer(ticker_weights)             
                # Enter a position
                if self.tickers_invested[ticker] == False:
                    long_signal = SignalEvent(ticker, "BOT")
                    # Select type of sizer and size the quantity of the signal
                    sized_signal = signal_sizer.size_signal(
                        portfolio_handler.portfolio, long_signal
                    )
                    # Add event to queue
                    if sized_signal.suggested_quantity > 0:
                        self.events_queue.put(sized_signal)
                elif self.tickers_invested[ticker]:
                    # Rebalance position
                    rebalance_signal = SignalEvent(ticker, "REBALANCE")
                    # Select type of sizer and size the quantity of the signal
                    sized_signal = signal_sizer.size_signal(
                        portfolio_handler.portfolio, rebalance_signal
                    )
                    # Add event to queue
                    if sized_signal.suggested_quantity > 0:
                        self.events_queue.put(sized_signal)

def run(config, testing, tickers, filename):
    # Backtest information
    title = [
        'Monthly Modern Portfolio Theory Rebalance on %s' % tickers[:]
    ]
    initial_equity = 10000.0
    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime(2020, 3, 20)
    
    # Acceptable drop percentage for the position
    risk_pct = 0.2

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = BuyAndHoldStopLossStrategy(
        tickers, events_queue, risk_pct
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
    tickers = ["SPY","GOOG", "AAPL"]
    filename = None
    run(config, testing, tickers, filename)
