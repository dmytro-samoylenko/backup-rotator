"""Logging configuration with monthly rotation."""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional


def setup_logger(
    log_directory: str = "logs",
    log_level: str = "INFO",
    log_name: str = "backup-rotator",
) -> logging.Logger:
    """Setup logger with monthly rotation.

    Args:
        log_directory: Directory for log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_name: Name for the logger and log file

    Returns:
        Configured logger instance
    """
    # Create log directory if it doesn't exist
    log_dir = Path(log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    logger = logging.getLogger(log_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # File handler with monthly rotation
    log_file = log_dir / f"{log_name}.log"
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when="M",  # Monthly rotation
        interval=1,
        backupCount=12,  # Keep 12 months
        encoding="utf-8",
    )

    # Console handler
    console_handler = logging.StreamHandler()

    # Formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger instance.

    Args:
        name: Logger name, defaults to "backup-rotator"

    Returns:
        Logger instance
    """
    return logging.getLogger(name or "backup-rotator")
