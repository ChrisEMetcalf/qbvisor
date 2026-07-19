# Changelog

This file records user-visible changes to qbvisor. The project follows
[Semantic Versioning](https://semver.org/) and keeps unreleased work under the `Unreleased`
heading.

## [Unreleased]

### Added

- Add public package metadata, inline typing, release and security policies, and distribution
  artifact validation ([#18]).
- Add trusted PyPI publishing with isolated OpenID Connect permissions, release identity checks,
  artifact attestations, and a documented maintainer procedure.
- Add task-focused guides, a generated public API reference, strict documentation validation, and
  the planned `0.3.0` migration.
- Add a shared synchronous transport with pooled connections, explicit timeouts, structured
  exceptions, and operation-aware retries ([#3]).
- Add app events, app roles, field usage, relationship lookup, relationship summary, and record
  change endpoints ([#5], [#11], [#12]).
- Add versioned application backups with manifests, attachment capture, integrity verification,
  and consistency checks ([#7]).
- Add declarative application and table schemas with reviewable plans, durable resource bindings,
  relationship management, and native Quickbase formula fields ([#8], [#10]).
- Add a tracked Quickbase OpenAPI response-shape manifest and opt-in persistent sandbox contract
  tests ([#2], [#3]).

### Changed

- Move app, table, field, and relationship operations behind private resource services while
  preserving `QuickBaseClient` as the public facade ([#6]).
- Return complete DataFrame queries and report results by default without changing their public
  method signatures ([#14]).
- Scan record exports in stable Record ID order and write through temporary files before publishing
  completed CSV files ([#13]).
- Batch record upserts by the exact serialized payload size, aggregate complete Quickbase outcomes,
  and report partially completed or uncertain ranges ([#16], [#17]).
- Use bounded asynchronous I/O for attachment downloads while preserving the existing synchronous
  entry points and result fields ([#4], [#15]).

### Fixed

- Preserve attachment bytes across response encodings and base64 conversion ([#9]).
- Continue attachment discovery until Quickbase metadata confirms that all matching records have
  been scanned ([#15]).
- Normalize record-change timestamps to conservative whole-second UTC values ([#12]).
- Invalidate and reuse table and field metadata without returning stale schema state ([#11]).

### Security

- Keep authorization values and request bodies out of transport logs and public exception messages
  ([#3]).
- Validate backup paths, attachment paths, and downloaded filenames before publishing files ([#7],
  [#15]).

## [0.2.0] - 2025-12-30

### Added

- Add small logging entry hooks and publish the `0.2.0` package metadata.

## [0.1.1] - 2025-06-19

### Changed

- Expand query options, lower attachment concurrency for reliability, and expose more upsert
  response information.

## [0.1.0] - 2025-05-13

### Added

- Publish the initial Quickbase client, query helpers, transport, tests, and project documentation.

[Unreleased]: https://github.com/ChrisEMetcalf/qbvisor/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ChrisEMetcalf/qbvisor/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/ChrisEMetcalf/qbvisor/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ChrisEMetcalf/qbvisor/releases/tag/v0.1.0
[#2]: https://github.com/ChrisEMetcalf/qbvisor/pull/2
[#3]: https://github.com/ChrisEMetcalf/qbvisor/pull/3
[#4]: https://github.com/ChrisEMetcalf/qbvisor/pull/4
[#5]: https://github.com/ChrisEMetcalf/qbvisor/pull/5
[#6]: https://github.com/ChrisEMetcalf/qbvisor/pull/6
[#7]: https://github.com/ChrisEMetcalf/qbvisor/pull/7
[#8]: https://github.com/ChrisEMetcalf/qbvisor/pull/8
[#9]: https://github.com/ChrisEMetcalf/qbvisor/pull/9
[#10]: https://github.com/ChrisEMetcalf/qbvisor/pull/10
[#11]: https://github.com/ChrisEMetcalf/qbvisor/pull/11
[#12]: https://github.com/ChrisEMetcalf/qbvisor/pull/12
[#13]: https://github.com/ChrisEMetcalf/qbvisor/pull/13
[#14]: https://github.com/ChrisEMetcalf/qbvisor/pull/14
[#15]: https://github.com/ChrisEMetcalf/qbvisor/pull/15
[#16]: https://github.com/ChrisEMetcalf/qbvisor/pull/16
[#17]: https://github.com/ChrisEMetcalf/qbvisor/pull/17
[#18]: https://github.com/ChrisEMetcalf/qbvisor/pull/18
