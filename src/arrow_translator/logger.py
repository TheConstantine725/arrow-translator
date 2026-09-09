import logging
from pathlib import Path


DEFAULT_LOGGER_NAME = "arrow-translator"
DEFAULT_LOGGING_CONFIG = {"logging": {"logger_name": DEFAULT_LOGGER_NAME}}
LOGGER_CONFIG_PATH: Path = Path.cwd().joinpath(".arrow-trans.toml")

LOGGER = logging.getLogger(DEFAULT_LOGGER_NAME)


def change_logger(logger: logging.Logger):
    LOGGER = logger
