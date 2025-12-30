from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    import coloredlogs
    COLOREDLOGS_INSTALLED = True
except ImportError:
    COLOREDLOGS_INSTALLED = False

# Tiny hook system for custom log handlers
Hook = Callable[..., None]

class LogHooks:
    _hooks: Dict[str, List[Hook]] = {
        "before_setup": [],
        "after_setup": [],
        "on_get_logger": [],
    }

    @classmethod
    def register(cls, hook_name: str, fn: Hook):
        if hook_name not in cls._hooks:
            raise ValueError(f"Unknown hook: {hook_name}")
        cls._hooks[hook_name].append(fn)

    @classmethod
    def run(cls, hook_name: str, **kwargs: Any):
        for fn in cls._hooks.get(hook_name, []):
            fn(**kwargs)

# Public API
_DEFAULT_LOGGER_NAME = "qbvisor"

def get_logger(name: str = _DEFAULT_LOGGER_NAME) -> logging.Logger:
    """
    Return a logger by name. Does not auto-configure.
    Call LoggingConfigurator.setup(...) in your script to configure output.
    """
    logger = logging.getLogger(name)
    LogHooks.run("on_get_logger", logger=logger)
    return logger

class LoggingConfigurator:
    """
    Optional setup utility for file + console logging.
    Does not auto-run — call `setup()` explicitly in your script.
    """

    _configured = False

    @classmethod
    def setup(
        cls,
        *,
        logger_name: str = _DEFAULT_LOGGER_NAME,
        log_dir: str = "logs",
        log_name: Optional[str] = None,
        log_level: Optional[str] = None,
        enable_colored: bool = True,
        fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        max_bytes: int = 5 * 1024 * 1024,
        backup_count: int = 5,
        clear_handlers: bool = False,
        propagate: bool = False,
    ) -> logging.Logger:
        """
        Configure logging for the specified logger name.
        Sets up both file and console logging.

        Args:
            logger_name: Name of the logger to configure.
            log_dir: Directory to store log files.
            log_name: Name of the log file. Defaults to '{logger_name}.log'.
            log_level: Logging level (e.g., 'DEBUG', 'INFO'). Defaults to 'INFO'.
            enable_colored: Whether to use colored logs in console (if coloredlogs is installed).
            fmt: Log message format.
            max_bytes: Maximum size of log file before rotation.
            backup_count: Number of backup log files to keep.
            clear_handlers: Whether to clear existing handlers before setup.
            propagate: Whether the logger should propagate messages to ancestor loggers.
        """

        if cls._configured:
            return logging.getLogger(logger_name)

        LogHooks.run(
            "before_setup", 
            logger_name=logger_name,
            log_dir=log_dir,
            log_name=log_name,
            log_level=log_level
            )

        # UTF-8 safety for log files
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")

        # Repo root (QBVisor/)
        project_root = Path(os.getenv("QBVISOR_ROOT", Path.cwd().resolve()))

        logfile = log_name or f'{project_root.name}.log'
        log_path = project_root / log_dir / logfile
        log_path.parent.mkdir(parents=True, exist_ok=True)

        level_str = (log_level or os.getenv("LOG_LEVEL", "DEBUG")).upper()
        level = getattr(logging, level_str, logging.DEBUG)

        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.propagate = propagate

        if clear_handlers:
            logger.handlers.clear()

        if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
            fh = RotatingFileHandler(
                log_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            fh.setFormatter(logging.Formatter(fmt))
            logger.addHandler(fh)

        if not any(
            isinstance(h, logging.StreamHandler)
            and not isinstance(h, RotatingFileHandler)
            for h in logger.handlers
        ):
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter(fmt))
            logger.addHandler(ch)

        if enable_colored and COLOREDLOGS_INSTALLED:
            coloredlogs.install(
                level=level_str,
                logger=logger,
                fmt=fmt,
            )
            
        cls._configured = True
        LogHooks.run(
            "after_setup",
            logger=logger,
            log_path=log_path,
        )
        return logger
    
def start_logging(**kwargs: Any) -> logging.Logger:
    """
    Convenience function to set up logging with default parameters.
    Calls LoggingConfigurator.setup(...) with any provided kwargs.
    """
    return LoggingConfigurator.setup(**kwargs)