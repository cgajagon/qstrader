from .base import AbstractSignalSizer


class NaiveSignalSizer(AbstractSignalSizer):
    def __init__(self, default_quantity=100):
        self.default_quantity = default_quantity

    def size_signal(self, portfolio, signal):
        """
        This NaiveSignalSizer object simply modifies
        the quantity to be a default quantity of any share transacted
        """
        signal.suggested_quantity = self.default_quantity

        return signal
