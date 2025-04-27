from contextlib import contextmanager
from abc import ABC, abstractmethod
import time

from reloadmanager.mixins.logging_mixin import LoggingMixin


class GenericDatabaseClient(ABC, LoggingMixin):
    def __init__(self, max_backoff_s: int = 300):
        self.MAX_BACKOFF_SECONDS = max_backoff_s

    @abstractmethod
    def _query(self, sql: str, headers: bool = False) -> list[tuple]:
        pass

    def _retryer(self, f, *args, max_attempts: int, **kwargs):
        attempt = 0
        delay = 1
        error: Exception = Exception()

        while attempt < max_attempts:
            try:
                return f(*args, **kwargs)

            # retry with exponential backoff to 30s
            except Exception as e:
                error = e
                attempt += 1
                self.logger.warning(f"Query failed (attempt {attempt}), retrying in {delay}s: {e}")
                time.sleep(delay)
                delay = min(delay * 2, self.MAX_BACKOFF_SECONDS)

        if attempt == 1:
            raise error

        raise TimeoutError(f"Query failed: max retries exceeded (attempt {attempt}). Most recent error: {repr(error)}")

    def query(self, sql: str, headers: bool = False, max_attempts: int = 1) -> list[tuple] | list[dict]:
        return self._retryer(self._query, sql, headers, max_attempts=max_attempts)
