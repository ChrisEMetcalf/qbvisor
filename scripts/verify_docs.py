"""Validate documentation links, Python examples, and public client coverage."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

MARKDOWN_LINK = re.compile(r"(?<!!)\[[^]]*]\(([^)]+)\)")
PYTHON_FENCE_START = re.compile(r"^\s*```python\s*$")
FENCE_END = re.compile(r"^\s*```\s*$")
REFERENCE_MEMBER = re.compile(r"^\s{8}- ([A-Za-z_]\w*)\s*$", re.MULTILINE)
EXTERNAL_SCHEMES = frozenset({"http", "https", "mailto"})
ROOT_MARKDOWN = ("README.md", "CONTRIBUTING.md", "CHANGELOG.md", "SECURITY.md")


def markdown_files(root: Path) -> list[Path]:
    """Return tracked documentation sources in stable display order."""
    files = [root / name for name in ROOT_MARKDOWN]
    files.extend(sorted((root / "docs").rglob("*.md")))
    return [path for path in files if path.is_file()]


def local_link_errors(root: Path, files: list[Path]) -> list[str]:
    """Report Markdown links whose local target does not exist."""
    errors: list[str] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for match in MARKDOWN_LINK.finditer(text):
            line_number = text.count("\n", 0, match.start()) + 1
            raw_target = match.group(1).strip()
            if raw_target.startswith("<") and raw_target.endswith(">"):
                raw_target = raw_target[1:-1]
            target = raw_target.split(maxsplit=1)[0]
            parsed = urlsplit(target)
            if parsed.scheme in EXTERNAL_SCHEMES or not parsed.path:
                continue
            resolved = (path.parent / unquote(parsed.path)).resolve()
            try:
                resolved.relative_to(root.resolve())
            except ValueError:
                errors.append(
                    f"{path.relative_to(root)}:{line_number}: local link escapes repository: "
                    f"{target}"
                )
                continue
            if not resolved.exists():
                errors.append(
                    f"{path.relative_to(root)}:{line_number}: missing local link target: {target}"
                )
    return errors


def python_example_errors(root: Path, files: list[Path]) -> list[str]:
    """Report fenced Python examples that are not syntactically valid."""
    errors: list[str] = []
    for path in files:
        lines = path.read_text(encoding="utf-8").splitlines()
        fence_start: int | None = None
        example: list[str] = []
        for line_number, line in enumerate(lines, 1):
            if fence_start is None:
                if PYTHON_FENCE_START.match(line):
                    fence_start = line_number
                    example = []
                continue
            if FENCE_END.match(line):
                try:
                    ast.parse("\n".join(example), filename=str(path))
                except SyntaxError as error:
                    detail = error.msg
                    if error.lineno is not None:
                        detail += f" at example line {error.lineno}"
                    errors.append(
                        f"{path.relative_to(root)}:{fence_start}: invalid Python example: {detail}"
                    )
                fence_start = None
                example = []
            else:
                example.append(line)
        if fence_start is not None:
            errors.append(f"{path.relative_to(root)}:{fence_start}: unclosed fenced Python example")
    return errors


def client_reference_errors(root: Path) -> list[str]:
    """Ensure every public client method is documented exactly once."""
    client_path = root / "src/qbvisor/client.py"
    reference_path = root / "docs/reference/client.md"
    module = ast.parse(client_path.read_text(encoding="utf-8"), filename=str(client_path))
    client = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == "QuickBaseClient"
    )
    public_methods = {
        node.name
        for node in client.body
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
    }
    missing_docstrings = sorted(
        node.name
        for node in client.body
        if isinstance(node, ast.FunctionDef)
        and not node.name.startswith("_")
        and ast.get_docstring(node) is None
    )
    reference_members = REFERENCE_MEMBER.findall(reference_path.read_text(encoding="utf-8"))
    referenced = set(reference_members)

    errors = [
        f"src/qbvisor/client.py: public method has no docstring: {name}"
        for name in missing_docstrings
    ]
    errors.extend(
        f"docs/reference/client.md: public client method is not referenced: {name}"
        for name in sorted(public_methods - referenced)
    )
    errors.extend(
        f"docs/reference/client.md: unknown client member is referenced: {name}"
        for name in sorted(referenced - public_methods)
    )
    duplicates = sorted({name for name in reference_members if reference_members.count(name) > 1})
    errors.extend(
        f"docs/reference/client.md: client member is referenced more than once: {name}"
        for name in duplicates
    )
    return errors


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    files = markdown_files(root)
    errors = [
        *local_link_errors(root, files),
        *python_example_errors(root, files),
        *client_reference_errors(root),
    ]
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(
        f"Documentation verification passed for {len(files)} Markdown files and "
        "the complete QuickBaseClient surface."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
