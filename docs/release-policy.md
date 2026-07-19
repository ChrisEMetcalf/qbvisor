# Release and compatibility policy

qbvisor uses [Semantic Versioning](https://semver.org/) for tagged releases.

## Versioning

- Patch releases contain backward-compatible fixes and documentation corrections.
- Minor releases add functionality. Before `1.0.0`, a minor release may also correct behavior that
  is clearly broken or inconsistent with Quickbase's documented API.
- Major releases may intentionally change public interfaces and require migration.

`QuickBaseClient` is the supported high-level interface. Existing method names and call signatures
remain stable when practical, including optional arguments retained for compatibility. If a
correction changes observable behavior, the changelog must explain the previous behavior, the new
behavior, and any required migration.

Modules whose names begin with an underscore are implementation details. They may change between
minor releases and should not be imported by consuming applications.

## Python support

Python 3.12 is the minimum supported version. Continuous integration verifies Python 3.12, 3.13,
and 3.14. A newer Python release is not considered supported until it is included in that matrix.

Dropping an actively supported Python version requires an announced minor release before `1.0.0`
or a major release after `1.0.0`.

## Installation

Until the first PyPI release is published, install the current development package from GitHub:

```bash
python -m pip install git+https://github.com/ChrisEMetcalf/qbvisor.git
```

Published distributions will become the recommended installation path after the initial PyPI
release. Releases will use reviewed commits, signed `vMAJOR.MINOR.PATCH` tags, validated wheel and
source artifacts, and a protected publishing environment.

Maintainers must follow the [release process](releasing.md). The publishing job uses PyPI trusted
publishing with a short-lived identity token; the repository does not store a PyPI credential.

Production applications should pin a released version. If a temporary Git dependency is required,
pin its full commit SHA rather than tracking `main`.

## Deprecation and migration

When practical, a public interface will remain available for at least one minor release after it is
deprecated. The warning, changelog, and release notes will identify its replacement. Security fixes
and corrections that prevent data loss may require a shorter migration window.

Every release should state:

- User-visible additions, changes, fixes, and security effects
- Public compatibility or migration concerns
- Supported Python versions
- Verification completed against built artifacts
- Known limitations that affect correctness, performance, or recovery
