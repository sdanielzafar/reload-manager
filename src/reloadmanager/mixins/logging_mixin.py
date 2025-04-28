import logging


class LoggingMixin:
    _fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(self._fmt)
        self.logger.addHandler(stream_handler)

    def set_logger_level(self, lvl: str):
        level = getattr(logging, lvl.upper(), None)
        if not isinstance(level, int):
            raise ValueError(f"Invalid log level: {lvl}")
        self.logger.setLevel(level)
