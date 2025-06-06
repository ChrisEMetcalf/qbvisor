import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    import coloredlogs
    COLOREDLOGS_INSTALLED = True
except ImportError:
    COLOREDLOGS_INSTALLED = False


def get_logger(name: str = "qbvisor") -> logging.Logger:
    """
    Return a logger by name. Does not auto-configure.
    Call LoggingConfigurator.setup(...) in your script to configure output.
    """
    return logging.getLogger(name)


class LoggingConfigurator:
    """
    Optional setup utility for file + console logging.
    Does not auto-run — call `setup()` explicitly in your script.
    """

    _configured = False

    @classmethod
    def setup(
        cls,
        log_name: str = None,
        log_dir: str = "logs",
        logger_name: str = None,
        enable_colored: bool = True,
        log_level: str = None
    ):
        if cls._configured:
            return

        # Force UTF-8 output on modern consoles
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")

        # Build log path
        project_root = Path(__file__).resolve().parents[1]
        logfile = log_name or f"{project_root.name}.log"
        log_path = project_root / log_dir / logfile
        log_path.parent.mkdir(parents=True, exist_ok=True)

        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        level = (log_level or os.getenv("LOG_LEVEL", "DEBUG")).upper()

        logger = logging.getLogger(logger_name or "qbvisor")
        logger.setLevel(getattr(logging, level, logging.DEBUG))

        # Rotating file handler
        fh = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter(fmt))
        logger.addHandler(fh)

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(fmt))
        logger.addHandler(ch)

        # Optional colored logs (if installed)
        if enable_colored and COLOREDLOGS_INSTALLED:
            coloredlogs.install(level=level, logger=logger, fmt=fmt)

        cls._configured = True
