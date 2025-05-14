"""Shared Logging setup for Common Pile."""

import functools
import logging
import sys
from typing import Protocol, Sequence

import contextual_logger
from logging_json import JSONFormatter


class GetFormatter(Protocol):
    def __call__(self) -> logging.Formatter:
        ...


class GetHandler(Protocol):
    def __call__(self) -> logging.Handler:
        ...


def get_json_formatter() -> logging.Formatter:
    fields = {
        "level_name": "levelname",
        "timestamp": "asctime",
        "module_name": "module",
        "function_name": "funcName",
        "logger": "name",
    }
    return JSONFormatter(fields=fields)


def get_stream_handler() -> logging.Handler:
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    return stream_handler


def get_file_handler(log_file: str) -> logging.Handler:
    file_handler = logging.FileHandler(log_file)
    return file_handler


# TODO: Add logging formatters that go to centralized places like
# datadog or watch tower.
DEFAULT_HANDLERS = (
    get_stream_handler,
    functools.partial(get_file_handler, log_file="common_pile_log.txt"),
)


def configure_logging(
    name: str = "common-pile",
    level: str = "INFO",
    get_formatter_fn: GetFormatter = get_json_formatter,
    handler_fns: Sequence[GetHandler] = DEFAULT_HANDLERS,
) -> logging.Logger:
    logger = logging.getLogger(name)
    level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level)

    formatter = get_formatter_fn()

    for handler_fn in handler_fns:
        handler = handler_fn()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def get_logger(name: str = "common-pile") -> logging.Logger:
    return logging.getLogger(name)
