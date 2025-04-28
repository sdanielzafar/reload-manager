import logging


class LoggingMixin:

    added_logger = False
    _fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    @property
    def logger(self):
        logger = logging.getLogger(__name__)

        if not type(self).added_logger:
            handler = logging.StreamHandler()
            handler.setFormatter(type(self)._fmt)
            logger.addHandler(handler)
            type(self).added_logger = True

        logger.propagate = False
        return logger

    def set_logger_level(self, log_level):
        level = getattr(logging, log_level.upper(), None)
        if not isinstance(level, int):
            raise ValueError(f"Invalid log level: {log_level}")

        self.logger.setLevel(level)
