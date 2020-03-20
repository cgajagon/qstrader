import psycopg2
import os
import pandas as pd
from datetime import date
from qtrade import Questrade

from ..price_parser import PriceParser
from .base import AbstractBarPriceHandler
from ..event import BarEvent


class QuestradeBarPriceHandler(AbstractBarPriceHandler):
    """
    QuestradeBarPriceHandler is designed to get from Questrade API
    dayly Open-High-Low-Close-Volume (OHLCV) data
    for each requested financial instrument and stream those to
    the provided events queue as BarEvents.
    """
    def __init__(
        self, events_queue,
        init_tickers=None,
        start_date=None, end_date=None,
        calc_returns=False
    ):
        """
        Takes the Questrade API, the events queue and a possible
        list of initial ticker symbols then creates an (optional)
        list of ticker subscriptions and associated prices.
        """
        self.events_queue = events_queue
        self.continue_backtest = True
        self.tickers = {}
        self.tickers_data = {}
        if init_tickers is not None:
            for ticker in init_tickers:
                self.subscribe_ticker(ticker)
        self.start_date = start_date
        self.end_date = end_date
        self.bar_stream = self._merge_sort_ticker_data()
        self.calc_returns = calc_returns
        if self.calc_returns:
            self.close_returns = []

    def _open_ticker_price(self, ticker):
        """
        Call the API and retrieve the containing the equities ticks, 
        converting them into a pandas DataFrame, stored in a dictionary.
        """     
        provider = Questrade(token_yaml="../access_token.yml")
        # retrieve all the history
        initial_date = "1950-01-01"
        end_date = date.today().strftime("%Y-%m-%d")
        data = provider.get_symbol_historical_data(ticker, initial_date, end_date, "OneDay")

        #print(self.start_date is None)
        # column names
        columns = [
            "Start","End","Low",
            "High","Open","Close","Volume","VWAP"
        ]
        # new column names
        new_columns = [
            "Start", "Open", "High", "Low",
            "Close", "Volume"
        ]
        
        self.tickers_data[ticker] = pd.DataFrame(data)
        print(self.tickers_data[ticker], ticker)
        self.tickers_data[ticker].columns = columns
        self.tickers_data[ticker]= self.tickers_data[ticker].drop(["End", "VWAP"], axis=1)
        self.tickers_data[ticker] = self.tickers_data[ticker][new_columns]
        self.tickers_data[ticker].Start = pd.to_datetime(self.tickers_data[ticker].Start, utc=True)
        self.tickers_data[ticker] = self.tickers_data[ticker].set_index('Start')
        self.tickers_data[ticker].index.name = "Date"
        self.tickers_data[ticker]['Ticker'] = ticker

    def _merge_sort_ticker_data(self):
        """
        Concatenates all of the separate equities DataFrames
        into a single DataFrame that is time ordered, allowing tick
        data events to be added to the queue in a chronological fashion.

        Note that this is an idealised situation, utilised solely for
        backtesting. In live trading ticks may arrive "out of order".
        """
        df = pd.concat(self.tickers_data.values()).sort_index()
        start = None
        end = None
        if self.start_date is not None:
            start = df.index.searchsorted(self.start_date)
        if self.end_date is not None:
            end = df.index.searchsorted(self.end_date)
        # This is added so that the ticker events are
        # always deterministic, otherwise unit test values
        # will differ
        df['colFromIndex'] = df.index
        df = df.sort_values(by=["colFromIndex", "Ticker"])
        if start is None and end is None:
            return df.iterrows()
        elif start is not None and end is None:
            return df.iloc[start:].iterrows()
        elif start is None and end is not None:
            return df.iloc[:end].iterrows()
        else:
            return df.iloc[start:end].iterrows()

    def subscribe_ticker(self, ticker):
        """
        Subscribes the price handler to a new ticker symbol.
        """
        if ticker not in self.tickers:
            try:
                self._open_ticker_price(ticker)
                dft = self.tickers_data[ticker]
                row0 = dft.iloc[0]

                close = PriceParser.parse(row0["Close"])

                ticker_prices = {
                    "close": close,
                    "timestamp": dft.index[0]
                }
                self.tickers[ticker] = ticker_prices

            except IndexError:
                print(
                    "Could not subscribe ticker %s "
                    "as no data found for pricing." % ticker
                )            
        else:
            print(
                "Could not subscribe ticker %s "
                "as is already subscribed." % ticker
            )

    def _create_event(self, index, period, ticker, row):
        """
        Obtain all elements of the bar from a row of dataframe
        and return a BarEvent
        """
        open_price = PriceParser.parse(row["Open"])
        high_price = PriceParser.parse(row["High"])
        low_price = PriceParser.parse(row["Low"])
        close_price = PriceParser.parse(row["Close"])
        volume = int(row["Volume"])
        bev = BarEvent(
            ticker, index, period, open_price,
            high_price, low_price, close_price,
            volume
        )
        return bev

    def _store_event(self, event):
        """
        Store price event for closing price and adjusted closing price
        """
        ticker = event.ticker
        # If the calc_adj_returns flag is True, then calculate
        # and store the full list of adjusted closing price
        # percentage returns in a list
        # TODO: Make this faster
        if self.calc_returns:
            prev_close = self.tickers[ticker][
                "close"
            ] / float(PriceParser.PRICE_MULTIPLIER)
            cur_close = event.close_price / float(
                PriceParser.PRICE_MULTIPLIER
            )
            self.tickers[ticker][
                "close_ret"
            ] = cur_close / prev_close - 1.0
            self.close_returns.append(self.tickers[ticker]["close_ret"])
        self.tickers[ticker]["close"] = event.close_price
        self.tickers[ticker]["timestamp"] = event.time

    def stream_next(self):
        """
        Place the next BarEvent onto the event queue.
        """
        try:
            index, row = next(self.bar_stream)
        except StopIteration:
            self.continue_backtest = False
            return
        # Obtain all elements of the bar from the dataframe
        ticker = row["Ticker"]
        period = 86400  # Seconds in a day
        # Create the tick event for the queue
        bev = self._create_event(index, period, ticker, row)
        # Store event
        self._store_event(bev)
        # Send event to queue
        self.events_queue.put(bev)