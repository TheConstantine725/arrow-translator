import logging
import sys

DEFAULT_LOGGER_NAME = "arrow-translator"
FILE_LOGGER = ".arrow-trans.log"

def create_default_logger():
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    file_handler = logging.FileHandler(encoding="utf-8", filename=FILE_LOGGER, mode="a")
    file_handler.setLevel(level=logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        fmt="{asctime}{msecs} [{levelname}] {name} || File: {filename} Function: {funcName}:{lineno} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger


LOGGER = create_default_logger()


def change_logger(logger: logging.Logger):
    LOGGER = logger


if LOGGER.name != DEFAULT_LOGGER_NAME:
    LOGGER.info(
        "Arrow-Translator initiallized with the non default logger: %s", LOGGER.name
    )
else:
    LOGGER.info("Arrow-Translator Initialized with default Settings")
