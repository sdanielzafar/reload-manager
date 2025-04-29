from threading import Lock, Event


class LogLock:
    lock = Lock()


class StopSignal:
    _event = Event()

    @classmethod
    def set(cls):
        cls._event.set()

    @classmethod
    def is_set(cls):
        cls._event.is_set()

    @classmethod
    def clear(cls):
        cls._event.clear()