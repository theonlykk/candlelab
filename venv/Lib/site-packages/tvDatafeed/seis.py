"""Symbol-Exchange-Interval Set (Seis) for tracking live data streams."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    import pandas as pd
    from .datafeed import TvDatafeedLive
    from .consumer import Consumer
    from .main import Interval


@dataclass
class Seis:
    """Symbol, exchange, and interval data set for live streaming.

    Holds a unique combination of symbol, exchange, and interval values,
    along with associated consumers for this data stream.

    Args:
        symbol: Ticker symbol string
        exchange: Exchange where symbol is listed
        interval: Chart time interval

    Example:
        >>> seis = Seis('ETHUSDT', 'BINANCE', Interval.in_1_hour)
        >>> seis.new_consumer(my_callback)
    """

    symbol: str
    exchange: str
    interval: Interval

    # Private fields not shown in repr
    _tvdatafeed: TvDatafeedLive | None = field(default=None, repr=False, init=False)
    _consumers: list[Consumer] = field(default_factory=list, repr=False, init=False)
    _updated: datetime.datetime | None = field(default=None, repr=False, init=False)

    def __eq__(self, other: object) -> bool:
        """Compare two Seis instances for equality.

        Instances are equal if symbol, exchange, and interval match.

        Args:
            other: Object to compare with

        Returns:
            True if equal, False otherwise
        """
        if not isinstance(other, Seis):
            return NotImplemented

        return (
            self.symbol == other.symbol and
            self.exchange == other.exchange and
            self.interval == other.interval
        )

    def __str__(self) -> str:
        """Return string representation."""
        return f"symbol='{self.symbol}', exchange='{self.exchange}', interval='{self.interval.name}'"

    @property
    def tvdatafeed(self) -> TvDatafeedLive | None:
        """Get associated TvDatafeedLive instance."""
        return self._tvdatafeed

    @tvdatafeed.setter
    def tvdatafeed(self, value: TvDatafeedLive) -> None:
        """Set TvDatafeedLive instance.

        Args:
            value: TvDatafeedLive instance

        Raises:
            AttributeError: If already set (cannot overwrite)
            TypeError: If value is not TvDatafeedLive instance
        """
        # Import here to avoid circular dependency
        from .datafeed import TvDatafeedLive as TvDatafeedLiveClass

        if self._tvdatafeed is not None:
            raise AttributeError("Cannot overwrite tvdatafeed - delete it first")

        if not isinstance(value, TvDatafeedLiveClass):
            raise TypeError("Value must be TvDatafeedLive instance")

        self._tvdatafeed = value

    @tvdatafeed.deleter
    def tvdatafeed(self) -> None:
        """Delete TvDatafeedLive reference."""
        self._tvdatafeed = None

    def new_consumer(
        self,
        callback: Callable[[Seis, pd.DataFrame], None],
        timeout: int = -1
    ) -> Consumer | bool:
        """Create new consumer and add to this Seis.

        Args:
            callback: Function(seis, data) to call with new data
            timeout: Maximum wait time in seconds (-1 for blocking)

        Returns:
            Consumer instance, or False if timeout

        Raises:
            RuntimeError: If no TvDatafeedLive reference set

        Example:
            >>> def my_callback(seis, data):
            ...     print(f"New data: {data}")
            >>> consumer = seis.new_consumer(my_callback)
        """
        if self._tvdatafeed is None:
            raise RuntimeError("No TvDatafeedLive instance associated with this Seis")

        return self._tvdatafeed.new_consumer(self, callback, timeout)

    def del_consumer(
        self,
        consumer: Consumer,
        timeout: int = -1
    ) -> bool:
        """Remove consumer from this Seis.

        Args:
            consumer: Consumer instance to remove
            timeout: Maximum wait time in seconds (-1 for blocking)

        Returns:
            True if successful, False if timeout

        Raises:
            RuntimeError: If no TvDatafeedLive reference set
        """
        if self._tvdatafeed is None:
            raise RuntimeError("No TvDatafeedLive instance associated with this Seis")

        return self._tvdatafeed.del_consumer(consumer, timeout)

    def add_consumer(self, consumer: Consumer) -> None:
        """Add consumer to internal list.

        Internal method used by TvDatafeedLive. Not for direct use.

        Args:
            consumer: Consumer instance to add
        """
        self._consumers.append(consumer)

    def pop_consumer(self, consumer: Consumer) -> None:
        """Remove consumer from internal list.

        Internal method used by TvDatafeedLive. Not for direct use.

        Args:
            consumer: Consumer instance to remove

        Raises:
            ValueError: If consumer not in list
        """
        if consumer not in self._consumers:
            raise ValueError("Consumer not registered with this Seis")
        self._consumers.remove(consumer)

    def is_new_data(self, data: pd.DataFrame) -> bool:
        """Check if data contains a new bar (different datetime).

        Args:
            data: DataFrame with datetime index

        Returns:
            True if new data, False if duplicate
        """
        current_dt = data.index.to_pydatetime()[0]

        if self._updated != current_dt:
            self._updated = current_dt
            return True

        return False

    def get_hist(
        self,
        n_bars: int = 10,
        timeout: int = -1
    ) -> pd.DataFrame | bool:
        """Get historical data for this Seis.

        Args:
            n_bars: Number of bars to retrieve
            timeout: Maximum wait time in seconds (-1 for blocking)

        Returns:
            DataFrame with OHLCV data, or False if timeout

        Raises:
            RuntimeError: If no TvDatafeedLive reference set

        Example:
            >>> data = seis.get_hist(n_bars=100)
        """
        if self._tvdatafeed is None:
            raise RuntimeError("No TvDatafeedLive instance associated with this Seis")

        return self._tvdatafeed.get_hist(
            symbol=self.symbol,
            exchange=self.exchange,
            interval=self.interval,
            n_bars=n_bars,
            timeout=timeout
        )

    def del_seis(self, timeout: int = -1) -> bool:
        """Remove this Seis from TvDatafeedLive.

        Args:
            timeout: Maximum wait time in seconds (-1 for blocking)

        Returns:
            True if successful, False if timeout

        Raises:
            RuntimeError: If no TvDatafeedLive reference set
        """
        if self._tvdatafeed is None:
            raise RuntimeError("No TvDatafeedLive instance associated with this Seis")

        return self._tvdatafeed.del_seis(self, timeout)

    def get_consumers(self) -> list[Consumer]:
        """Get list of all consumers for this Seis.

        Returns:
            List of Consumer instances
        """
        return self._consumers
