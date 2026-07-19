# qbvisor documentation

qbvisor is a developer-focused Python SDK for treating Quickbase as an application backend. It
combines direct JSON API access with label-based field resolution, complete DataFrame reads,
payload-aware writes, verified backups, and declarative application schemas.

The [repository README](https://github.com/ChrisEMetcalf/qbvisor#readme) provides the shortest
installation and first-query path. Use this site for implementation and operational detail.

## Start here

- [Configuration and authentication](configuration.md)
- [Data queries, reports, and record movement](data-workflows.md)
- [Building Quickbase applications](application-building.md)
- [Backups and attachments](backups-and-attachments.md)
- [Logging, errors, and recovery](logging-and-errors.md)
- [Declarative application schemas](declarative-schemas.md)
- [Public Python API](reference/index.md)
- [Quickbase endpoint coverage](api/README.md)
- [Upgrading from 0.2 to 0.3](upgrading-to-0.3.md)

## Public contract

`QuickBaseClient` is the supported high-level interface. The [public API
reference](reference/index.md) separates its primary workflows from compatibility helpers and
lower-level transport control. Modules beginning with an underscore are implementation details.

Methods that mutate Quickbase are identified in their guides together with retry, idempotency,
and recovery implications.

## Project policies

- [Release and compatibility policy](release-policy.md)
- [Maintainer release process](releasing.md)
- [Changelog](https://github.com/ChrisEMetcalf/qbvisor/blob/main/CHANGELOG.md)
- [Security policy](https://github.com/ChrisEMetcalf/qbvisor/blob/main/SECURITY.md)
- [Contributing](https://github.com/ChrisEMetcalf/qbvisor/blob/main/CONTRIBUTING.md)
