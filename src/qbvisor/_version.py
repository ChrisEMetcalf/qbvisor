"""Installed qbvisor package version."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("qbvisor")
except PackageNotFoundError:  # pragma: no cover - only possible from an uninstalled source tree
    __version__ = "0+unknown"
