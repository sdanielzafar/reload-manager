import logging


class LoggingMixin:
    @property
    def logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.getLogger().getEffectiveLevel())

        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setLevel(logging.getLogger().getEffectiveLevel())
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        logger.propagate = False
        return logger

    def set_logger_level(self, log_level):
        level = getattr(logging, log_level.upper(), None)
        if not isinstance(level, int):
            raise ValueError(f"Invalid log level: {log_level}")

        self.logger.setLevel(level)
