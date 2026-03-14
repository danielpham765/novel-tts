from __future__ import annotations

import logging
import sys
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logging.getLogger().handlers:
        configure_logging()
    return logger


def configure_logging(log_file: Path | None = None, level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # Reset handlers so CLI can switch log targets per run.
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def get_novel_log_dir(logs_root: Path, novel_id: str) -> Path:
    return logs_root / novel_id


def get_novel_log_path(logs_root: Path, novel_id: str, log_name: str) -> Path:
    return get_novel_log_dir(logs_root, novel_id) / log_name


def install_exception_logging(logger: logging.Logger) -> None:
    def _hook(exc_type, exc_value, exc_traceback) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.exception("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = _hook
