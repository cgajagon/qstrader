from qstrader.strategy.base import AbstractStrategy
from qstrader.event import SignalEvent, EventType
from qstrader.compat import queue
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