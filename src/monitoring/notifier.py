from __future__ import annotations

import logging


class Notifier:
    """MVP notifier (stdout logger)."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def info(self, message: str) -> None:
        self.logger.info(message)

    def error(self, message: str) -> None:
        self.logger.error(message)
