"""Validate qbvisor wheel and source-distribution contents before release."""

from __future__ import annotations

import argparse
import re
import tarfile
import tomllib
import zipfile
from collections.abc import Iterable
from email.message import Message
from email.parser import BytesParser
from email.policy import default
from pathlib import Path, PurePosixPath

EXPECTED_CLASSIFIERS = {
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Typing :: Typed",
}
EXPECTED_PROJECT_URLS = {
    "Changelog": "https://github.com/ChrisEMetcalf/qbvisor/blob/main/CHANGELOG.md",
    "Documentation": "https://chrisemetcalf.github.io/qbvisor/",
    "Issues": "https://github.com/ChrisEMetcalf/qbvisor/issues",
    "Repository": "https://github.com/ChrisEMetcalf/qbvisor",
}
REQUIRED_SDIST_FILES = {
    "CHANGELOG.md",
    "LICENSE.md",
    "README.md",
    "SECURITY.md",
    "pyproject.toml",
    "src/qbvisor/__init__.py",
    "src/qbvisor/py.typed",
}
REQUIRED_WHEEL_FILES = {
    "qbvisor/__init__.py",
    "qbvisor/py.typed",
}
REJECTED_DIRECTORIES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".qbvisor",
    ".ruff_cache",
    ".venv",
    ".venv.nosync",
    "__pycache__",
    "build",
    "dist",
    "htmlcov",
    "logs",
    "temp",
    "tmp",
}
REJECTED_SUFFIXES = {".log", ".pyc", ".pyo"}
RELEASE_TAG = re.compile(r"^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


class DistributionValidationError(ValueError):
    """Raised when a built distribution violates the release contract."""


def validate_release_identity(tag: str, project_file: Path, distribution_version: str) -> str:
    """Require one final-release version across the tag, project, and artifacts."""
    match = RELEASE_TAG.fullmatch(tag)
    if match is None:
        raise DistributionValidationError(
            f"Release tag must use vMAJOR.MINOR.PATCH with no leading zeroes; found {tag!r}"
        )
    tag_version = ".".join(match.groups())

    with project_file.open("rb") as stream:
        project = tomllib.load(stream)
    try:
        project_version = project["project"]["version"]
    except (KeyError, TypeError) as error:
        raise DistributionValidationError(
            f"Project file {project_file} does not define project.version"
        ) from error
    if not isinstance(project_version, str) or not project_version:
        raise DistributionValidationError(
            f"Project file {project_file} must define project.version as a non-empty string"
        )

    versions = {
        "release tag": tag_version,
        "project": project_version,
        "distribution": distribution_version,
    }
    if len(set(versions.values())) != 1:
        rendered = ", ".join(f"{source}={version!r}" for source, version in versions.items())
        raise DistributionValidationError(f"Release versions do not match: {rendered}")
    return tag_version


def _require_single(paths: Iterable[Path], description: str) -> Path:
    matches = sorted(paths)
    if len(matches) != 1:
        names = ", ".join(path.name for path in matches) or "none"
        raise DistributionValidationError(
            f"Expected one {description}; found {len(matches)}: {names}"
        )
    return matches[0]


def _validate_archive_paths(paths: Iterable[str]) -> None:
    rejected: list[str] = []
    for value in paths:
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            rejected.append(value)
            continue

        name = path.name
        contains_rejected_directory = any(part in REJECTED_DIRECTORIES for part in path.parts)
        contains_private_env = name == ".env" or (
            name.startswith(".env.") and name != ".env.example"
        )
        contains_generated_file = (
            name == ".DS_Store" or name.startswith(".coverage") or path.suffix in REJECTED_SUFFIXES
        )
        if contains_rejected_directory or contains_private_env or contains_generated_file:
            rejected.append(value)

    if rejected:
        rendered = "\n- ".join(rejected)
        raise DistributionValidationError(
            f"Distribution contains local or generated artifacts:\n- {rendered}"
        )


def _metadata_from_wheel(archive: zipfile.ZipFile) -> Message:
    metadata_path = _require_single(
        (Path(name) for name in archive.namelist() if name.endswith(".dist-info/METADATA")),
        "wheel METADATA file",
    )
    return BytesParser(policy=default).parsebytes(archive.read(metadata_path.as_posix()))


def _validate_metadata(metadata: Message, wheel: Path, sdist: Path) -> str:
    expected_fields = {
        "Name": "qbvisor",
        "License-Expression": "MIT",
        "Requires-Python": ">=3.12",
    }
    for field, expected in expected_fields.items():
        actual = metadata.get(field)
        if actual != expected:
            raise DistributionValidationError(
                f"Metadata {field} must be {expected!r}; found {actual!r}"
            )

    version = metadata.get("Version")
    if not version:
        raise DistributionValidationError("Distribution metadata does not define Version")
    if not wheel.name.startswith(f"qbvisor-{version}-"):
        raise DistributionValidationError(
            f"Wheel name {wheel.name!r} does not match metadata version {version!r}"
        )
    if sdist.name != f"qbvisor-{version}.tar.gz":
        raise DistributionValidationError(
            f"Source archive name {sdist.name!r} does not match metadata version {version!r}"
        )

    classifiers = set(metadata.get_all("Classifier", []))
    missing_classifiers = EXPECTED_CLASSIFIERS - classifiers
    if missing_classifiers:
        raise DistributionValidationError(
            f"Distribution metadata is missing classifiers: {sorted(missing_classifiers)}"
        )

    project_urls = {
        label.strip(): url.strip()
        for value in metadata.get_all("Project-URL", [])
        for label, separator, url in [value.partition(",")]
        if separator
    }
    for label, expected in EXPECTED_PROJECT_URLS.items():
        actual = project_urls.get(label)
        if actual != expected:
            raise DistributionValidationError(
                f"Project URL {label} must be {expected!r}; found {actual!r}"
            )

    license_files = set(metadata.get_all("License-File", []))
    if "LICENSE.md" not in license_files:
        raise DistributionValidationError("Distribution metadata does not include LICENSE.md")

    return version


def validate_distributions(directory: Path) -> str:
    """Validate exactly one wheel and source distribution in ``directory``."""

    wheel = _require_single(directory.glob("*.whl"), "wheel")
    sdist = _require_single(directory.glob("*.tar.gz"), "source distribution")

    with zipfile.ZipFile(wheel) as archive:
        wheel_paths = set(archive.namelist())
        _validate_archive_paths(wheel_paths)
        missing_wheel_files = REQUIRED_WHEEL_FILES - wheel_paths
        if missing_wheel_files:
            raise DistributionValidationError(
                f"Wheel is missing required files: {sorted(missing_wheel_files)}"
            )
        metadata = _metadata_from_wheel(archive)

    version = _validate_metadata(metadata, wheel, sdist)

    with tarfile.open(sdist, mode="r:gz") as archive:
        members = archive.getmembers()
        if any(member.issym() or member.islnk() for member in members):
            raise DistributionValidationError("Source distribution must not contain links")
        sdist_paths = {member.name for member in members}
        _validate_archive_paths(sdist_paths)

    root = f"qbvisor-{version}"
    missing_sdist_files = {
        path for path in REQUIRED_SDIST_FILES if f"{root}/{path}" not in sdist_paths
    }
    if missing_sdist_files:
        raise DistributionValidationError(
            f"Source distribution is missing required files: {sorted(missing_sdist_files)}"
        )

    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        type=Path,
        nargs="?",
        default=Path("dist"),
        help="Directory containing exactly one wheel and one .tar.gz source distribution",
    )
    parser.add_argument(
        "--release-tag",
        help="Release tag to compare with project and distribution versions",
    )
    parser.add_argument(
        "--project-file",
        type=Path,
        default=Path("pyproject.toml"),
        help="Project metadata used with --release-tag",
    )
    args = parser.parse_args()

    try:
        version = validate_distributions(args.directory)
        if args.release_tag is not None:
            validate_release_identity(args.release_tag, args.project_file, version)
    except (
        DistributionValidationError,
        OSError,
        tarfile.TarError,
        tomllib.TOMLDecodeError,
        zipfile.BadZipFile,
    ) as error:
        parser.exit(1, f"Distribution validation failed: {error}\n")

    print(f"Validated qbvisor {version} wheel and source distribution")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
