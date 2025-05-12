# modules/helpers.py

import re
import shutil
from pathlib import Path
from datetime import datetime
from modules.log_runner import get_logger

# Utilize the name of the module for logging
logger = get_logger(__name__)

def sanitize_filenames(filename: str) -> str:
    """
    Sanitize the filename by removing illegal characters.
    """
    # Define the regex pattern for illegal characters
    return re.sub(r'[<>:"/\\|?*]', '', filename)

def ensure_temp_dir(path: str, clean: bool = False) -> Path:
    """
    Ensure the temporary directory exists. If clean is True, remove existing files.
    """
    temp_path = Path(path)
    if clean and temp_path.exists():
        shutil.rmtree(temp_path)
    temp_path.mkdir(parents=True, exist_ok=True)
    return temp_path

def generate_timestamped_folder(base_dir: str, prefix: str = '', suffix: str = '', fmt: str = '%Y-%m-%d') -> str:
    """
    Generate a timestamped folder (eg: '2025-04-06') with optional prefix and suffix.
    """
    date_str = datetime.now().strftime(fmt)
    parts = [prefix, date_str, suffix]
    name = '_'.join([part for part in parts if part])
    path = Path(base_dir) / name
    path.mkdir(parents=True, exist_ok=True)
    return path

def summarize_file_sizes(folder: str) -> int:
    """
    Return the total size in bytes of all files in the folder.
    """
    total = sum(f.stat().st_size for f in Path(folder).glob('*') if f.is_file())
    logger.info(f"Total size of files in {folder}: {total} bytes")
    return total