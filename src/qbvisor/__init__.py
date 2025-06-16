"""
src/qbvisor/__init__.py

Aggregate exports for the QuickBase archiver package.
"""

from pathlib import Path
from dotenv import load_dotenv

# Try to load .env from common locations
possible_paths = [
    Path(__file__).resolve().parents[2] / ".env",  # repo root if src/qbvisor/
    Path(__file__).resolve().parents[1] / ".env",  # one level up
    Path.cwd() / ".env",                           # where the script is run from
]

for dotenv_path in possible_paths:
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
        break

from .client import QuickBaseClient
from .query_helper import QueryHelper
from .helpers import (
    sanitize_filenames,
    ensure_temp_dir,
    generate_timestamped_folder,
    summarize_file_sizes
)

from .log_runner import LoggingConfigurator, get_logger

# Expose file download utilities directly on the client

__all__ = [
    "QuickBaseClient",
    "QueryHelper",
    "sanitize_filenames",
    "ensure_temp_dir",
    "generate_timestamped_folder",
    "summarize_file_sizes",
    "LoggingConfigurator",
    "get_logger"
]