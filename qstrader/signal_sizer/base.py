from abc import ABCMeta, abstractmethod


class AbstractSignalSizer(object):
    """
    The AbstractSignalSizer abstract class modifies
    the quantity (or not) of any share transacted
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def size_signal(self, portfolio, signal):
        """
        This TestSignalSizer object simply modifies
        the quantity to be 100 of any share transacted.
        """
        raise NotImplementedError("Should implement size_signal()")
