import logging

from .formatters import ConsoleFormatter, JsonLinesFormatter

DEFAULT_LOGGER_NAME = "arrow-translator"
FILE_LOGGER = ".arrow-trans.log"


def _build_default_logger():
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    file_handler = logging.FileHandler(encoding="utf-8", filename=FILE_LOGGER, mode="a")
    file_handler.setLevel(level=logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(JsonLinesFormatter())
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger


_ARROW_TRANSLATOR_LOGGER_NAME = DEFAULT_LOGGER_NAME
_ARROW_TRANSLATOR_DEFAULT_LOGGER = _build_default_logger()
_ACTIVE_ARROW_TRANSLATOR_LOGGER = _ARROW_TRANSLATOR_DEFAULT_LOGGER


def change_logger(logger: logging.Logger):
    global _ACTIVE_ARROW_TRANSLATOR_LOGGER, _ARROW_TRANSLATOR_LOGGER_NAME
    _ACTIVE_ARROW_TRANSLATOR_LOGGER, _ARROW_TRANSLATOR_LOGGER_NAME = logger, logger.name


def get_logger() -> logging.Logger:
    return _ACTIVE_ARROW_TRANSLATOR_LOGGER
