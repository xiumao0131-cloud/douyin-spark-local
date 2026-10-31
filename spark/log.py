"""日志配置：控制台 + logs/spark.log。"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from spark import config


def configure_logging(level=logging.INFO):
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    root.addHandler(stream)
    file_handler = RotatingFileHandler(
        config.LOG_DIR / "spark.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
