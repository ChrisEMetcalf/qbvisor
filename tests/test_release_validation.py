import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_distribution.py"
SPEC = importlib.util.spec_from_file_location("verify_distribution", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
VALIDATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VALIDATOR)

DistributionValidationError = VALIDATOR.DistributionValidationError
validate_release_identity = VALIDATOR.validate_release_identity


def write_project(path: Path, version: object = "0.3.0") -> Path:
    project_file = path / "pyproject.toml"
    if isinstance(version, str):
        rendered_version = f'"{version}"'
    else:
        rendered_version = str(version)
    project_file.write_text(
        f'[project]\nname = "qbvisor"\nversion = {rendered_version}\n',
        encoding="utf-8",
    )
    return project_file


def test_release_identity_accepts_matching_final_version(tmp_path: Path):
    project_file = write_project(tmp_path)

    assert validate_release_identity("v0.3.0", project_file, "0.3.0") == "0.3.0"


@pytest.mark.parametrize("tag", ["0.3.0", "v0.3", "v0.03.0", "v0.3.0rc1"])
def test_release_identity_rejects_unsupported_tag_format(tmp_path: Path, tag: str):
    project_file = write_project(tmp_path)

    with pytest.raises(DistributionValidationError, match="vMAJOR.MINOR.PATCH"):
        validate_release_identity(tag, project_file, "0.3.0")


@pytest.mark.parametrize(
    ("project_version", "distribution_version"),
    [("0.2.0", "0.3.0"), ("0.3.0", "0.2.0")],
)
def test_release_identity_rejects_version_mismatch(
    tmp_path: Path,
    project_version: str,
    distribution_version: str,
):
    project_file = write_project(tmp_path, project_version)

    with pytest.raises(DistributionValidationError, match="Release versions do not match"):
        validate_release_identity("v0.3.0", project_file, distribution_version)


def test_release_identity_requires_string_project_version(tmp_path: Path):
    project_file = write_project(tmp_path, 3)

    with pytest.raises(DistributionValidationError, match="non-empty string"):
        validate_release_identity("v0.3.0", project_file, "0.3.0")
