from logging import INFO, StreamHandler, getLogger

from pythonjsonlogger import jsonlogger


def configure_logging() -> None:
    logger = getLogger()
    logger.handlers.clear()
    handler = StreamHandler()
    fmt = jsonlogger.JsonFormatter()
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(INFO)
