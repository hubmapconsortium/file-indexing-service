"""Shared timing instrumentation for the file-indexing-service scripts.

TimedStep is a context manager that logs how long a block of code takes.
It logs at INFO level only when the elapsed time exceeds a threshold, so
routine fast operations stay out of the log while slow ones are surfaced.

Usage:
    from timed_step import TimedStep

    with TimedStep(logger, "ES bulk insert 1000 docs"):
        bulk_insert_es_indices(...)

The threshold defaults to 0.1 seconds and can be overridden per use:

    with TimedStep(logger, "quick check", threshold=0.0):  # always log
        ...
"""
import time
from logging import Logger


class TimedStep:
    def __init__(self, logger: Logger, label: str, threshold: float = 0.1):
        self._logger = logger
        self._label = label
        self._threshold = threshold
        self._start = None

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self._start
        if elapsed > self._threshold:
            self._logger.info(f"TIMING: {self._label} took {elapsed:.2f}s")
        # Returning False (implicit) so any exception propagates normally.
        return False
