import datetime
import numpy as np
import pandas as pd

from qstrader import settings
from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
from qstrader.trading_session import TradingSession


class TortugaStrategy(AbstractStrategy):
    """
    TODO
    """
    def __init__(
        self, ticker, events_queue,
        base_quantity=100, long_window=30
    ):
        self.ticker = ticker
        self.events_queue = events_queue
        self.base_quantity = base_quantity
        self.bars = 0
        self.invested = False
        self.window = 30
        self.max_price = float('-inf')
        self.min_price = float('inf')
        self.support = float('inf')
        self.stop_loss = float('-inf')

    
    def calculate_signals(self, event):

        if (
            event.type in [EventType.BAR, EventType.TICK] and
            event.ticker == self.ticker
        ):              
        

            if self.invested == False:
                if self.max_price > float('-inf') and self.support < float('inf') and event.high_price > self.max_price:
                    order = event.high_price
                    self.stop_loss = self.support - self.support * 0.03
                    print("LONG %s: %s, at %s with stop at %s" % (self.ticker, event.time, order, self.stop_loss))    
                    signal = SignalEvent(
                        self.ticker, "BOT",
                        suggested_quantity=self.base_quantity
                    )
                    self.events_queue.put(signal)
                    self.invested = True

            if event.low_price < self.support:
                    self.support = event.low_price
                    #print('Update Support', self.support)
                
            if self.invested == True:
                if event.high_price > self.max_price and self.support > self.stop_loss:
                    self.stop_loss = self.support - self.support * 0.03
                    #print('Update Stop Loss',self.stop_loss)

                if self.support < self.stop_loss:
                    print("SHORT %s: %s, at %s" % (self.ticker, event.time, self.stop_loss))
                    signal = SignalEvent(
                        self.ticker, "SLD",
                        suggested_quantity=self.base_quantity
                    )
                    self.events_queue.put(signal)
                    self.invested = False

            if event.high_price > self.max_price:
                self.max_price = event.high_price
                self.support = float('inf')
                #print('Update Max', self.max_price)

            self.bars += 1


def run(config, testing, tickers, filename):
    # Backtest information
    title = ['Tortuga Example on %s' % tickers[0]]
    initial_equity = 3000.0
    start_date = datetime.datetime(2018, 12, 6)
    end_date = datetime.datetime(2020, 1, 1)

    # Use the Buy and Hold Strategy
    events_queue = queue.Queue()
    strategy = TortugaStrategy(tickers[0], events_queue)

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