"""Consumer thread for processing live data with callback functions."""

from __future__ import annotations

import logging
import queue
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    import pandas as pd
    from .seis import Seis

logger = logging.getLogger(__name__)


class Consumer(threading.Thread):
    """Threaded consumer for processing Seis data via callback.

    Runs in a separate thread to process incoming data bars from a Seis
    by calling a user-provided callback function. Uses a queue for
    thread-safe data passing.

    Args:
        seis: Seis instance this consumer processes data from
        callback: Function(seis, data) to call with new data bars

    Example:
        >>> def my_callback(seis, data):
        ...     print(f"Received bar: {data}")
        >>> consumer = Consumer(seis, my_callback)
        >>> consumer.start()
        >>> consumer.put(new_data)
    """

    def __init__(
        self,
        seis: Seis,
        callback: Callable[[Seis, pd.DataFrame], None]
    ) -> None:
        """Initialize consumer thread.

        Args:
            seis: Seis to consume data from
            callback: Callback function for new data
        """
        super().__init__(daemon=True)

        self._buffer: queue.Queue[pd.DataFrame | None] = queue.Queue()
        self.seis = seis
        self.callback = callback

        # Create descriptive thread name
        self.name = (
            f"{self.callback.__name__}_"
            f"{self.seis.symbol}_"
            f"{self.seis.exchange}_"
            f"{self.seis.interval.value}"
        )

    def __repr__(self) -> str:
        """Return detailed representation."""
        return f"Consumer({self.seis!r}, {self.callback.__name__})"

    def __str__(self) -> str:
        """Return string representation."""
        return f"{self.seis}, callback={self.callback.__name__}"

    def run(self) -> None:
        """Main thread loop - processes data from queue.

        Continuously pulls data from the queue and calls the callback
        function. Exits when None is received (shutdown signal).
        """
        logger.debug("Consumer thread %s started", self.name)

        while True:
            data = self._buffer.get()

            # None signals shutdown
            if data is None:
                logger.debug("Consumer thread %s received shutdown signal", self.name)
                break

            try:
                self.callback(self.seis, data)
            except Exception as e:
                logger.error(
                    "Callback %s raised exception for %s: %s",
                    self.callback.__name__,
                    self.seis,
                    e,
                    exc_info=True
                )
                # Clean up and exit on callback error
                try:
                    self.del_consumer()
                except Exception:
                    pass
                break

        # Cleanup references
        logger.debug("Consumer thread %s exiting", self.name)
        self.seis = None  # type: ignore
        self.callback = None  # type: ignore
        self._buffer = None  # type: ignore

    def put(self, data: pd.DataFrame | None) -> None:
        """Add data to processing queue.

        Args:
            data: DataFrame with bar data, or None to signal shutdown
        """
        if self._buffer is not None:
            self._buffer.put(data)

    def del_consumer(self, timeout: int = -1) -> bool:
        """Stop thread and remove from Seis.

        Args:
            timeout: Maximum wait time in seconds (-1 for blocking)

        Returns:
            True if successful, False if timeout
        """
        if self.seis is None:
            return True
        return self.seis.del_consumer(self, timeout)

    def stop(self) -> None:
        """Signal thread to stop processing.

        Sends None to queue which triggers thread exit.
        """
        if self._buffer is not None:
            self._buffer.put(None)
