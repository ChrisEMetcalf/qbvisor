# modules/log_runner.py

import os
import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

try:
    import coloredlogs
    COLOREDLOGS_INSTALLED = True
except ImportError:
    COLOREDLOGS_INSTALLED = False

class LoggingConfigurator:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, log_name: str = None, log_dir: str = 'logs', logger_name: str = None):
        # Force UTF‑8 encoding on console streams (Python 3.7+)
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")

        # Only add handlers on the very first instantiation
        if not getattr(self, '_handlers_added', False):
            # Build the file path: project-root/logs/<log_name or project-root.log>
            project_root = Path(__file__).resolve().parents[1]
            logfile = log_name or f"{project_root.name}.log"
            log_path = project_root / log_dir / logfile
            log_path.parent.mkdir(parents=True, exist_ok=True)

            # Standard format and level
            fmt   = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            level = os.getenv('LOG_LEVEL', 'DEBUG').upper()

            # Configure the root logger
            root = logging.getLogger()
            root.setLevel(getattr(logging, level, logging.DEBUG))

            # Rotating file handler
            fh = RotatingFileHandler(
            log_path,
            maxBytes=5*1024*1024,
            backupCount=5,
            encoding="utf-8"
            )

            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter(fmt))
            root.addHandler(fh)

            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(logging.Formatter(fmt))
            root.addHandler(ch)

            # Colored logs on console
            if COLOREDLOGS_INSTALLED:
                coloredlogs.install(level=level, logger=root, fmt=fmt)

            # Optional: propagate to urllib3 so you see HTTP debug too
            urllib_logger = logging.getLogger('urllib3.connectionpool')
            urllib_logger.setLevel(logging.DEBUG)
            if not urllib_logger.handlers:
                urllib_logger.addHandler(fh)
                urllib_logger.addHandler(ch)

            self._handlers_added = True

        # Each time, reset .logger to the requested name
        # If logger_name is None, fall back to this module’s name
        self.logger = logging.getLogger(logger_name or __name__)


def get_logger(name: str):
    """
    Return a singleton-configured logger named `name`.
    Use this at the top of every module or script:
        logger = get_logger(__name__)
    """
    return LoggingConfigurator(logger_name=name).logger
