# Releasing qbvisor

This procedure is for qbvisor maintainers. Package publication is deliberately separate from pull
request CI: reviewed code is merged first, a signed version tag identifies the release, and
publishing a GitHub Release authorizes one immutable PyPI upload.

## Trust boundary

`.github/workflows/release.yml` runs only when a final GitHub Release is published.

1. The build job checks out the release tag and confirms that its commit belongs to `main`.
2. The tag, `pyproject.toml`, wheel metadata, source archive, and filenames must all contain the
   same final `MAJOR.MINOR.PATCH` version.
3. The build job validates the distributions without access to an OpenID Connect token.
4. The publishing job downloads only those artifacts. It receives `id-token: write` and exchanges
   the short-lived GitHub identity for temporary PyPI credentials.
5. PyPI publishes provenance attestations with the distributions.

The repository must not contain a PyPI API token or publishing secret. Action dependencies in the
release workflow are pinned to full commit SHAs.

## One-time GitHub setup

After the release workflow has merged into `main`:

1. Open **Repository settings → Environments**.
2. Create an environment named exactly `pypi`.
3. Restrict deployment branches and tags to tags matching `v*`.
4. Do not add package credentials or other environment secrets.

While the project has one maintainer, publishing the GitHub Release is the manual authorization
gate. When another maintainer can perform release review, add that maintainer as a required
environment reviewer and prevent self-review.

## One-time PyPI setup

The trusted-publisher identity must match the workflow exactly:

| Setting | Value |
| --- | --- |
| PyPI project | `qbvisor` |
| GitHub owner | `ChrisEMetcalf` |
| Repository | `qbvisor` |
| Workflow filename | `release.yml` |
| Environment | `pypi` |

If `qbvisor` already appears under **Your projects**, open the project's **Publishing** page and
add a GitHub Actions trusted publisher with those values.

If the project does not exist publicly, open the account-level **Publishing** page and create a
pending publisher with the same values and project name. A pending publisher creates the project
on its first successful upload. It does not reserve the project name before that upload, so set it
up immediately before the first release.

## Prepare a release

Prepare each version through a focused pull request:

1. Set `[project].version` in `pyproject.toml` and refresh `uv.lock`.
2. Move the relevant changelog entries from `Unreleased` into a dated version section.
3. Finalize the matching migration guide and installation instructions.
4. Run all local quality, documentation, package, and persistent-sandbox contract checks.
5. Merge only after CI passes and the pull request has the required review.

The release commit on `main` must remain unchanged after this review.

For a patch release that changes runtime behavior, run the live checks against a quiet dedicated
sandbox in this order:

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=0 \
  QBVISOR_RUN_WORKLOADS=0 uv run pytest -m integration --no-cov
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  QBVISOR_RUN_WORKLOADS=0 \
  uv run pytest -m "integration and not workload" --no-cov
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  QBVISOR_RUN_WORKLOADS=1 QBVISOR_WORKLOAD_PROFILE=standard \
  uv run pytest -m workload --no-cov -s
```

Review the newest `.qbvisor/workloads/*.json` before approval. It must report `status: passed`,
`backupConsistent: true`, equal created, queried, exported, backed-up, and deleted counts, and no
failure phase. Attach or summarize that credential-free result in the release pull request. Use
`smoke` while developing and reserve `scale` for an intentional, separately reported exercise.

## Tag and publish

Synchronize `main`, create a signed annotated tag, and push the tag:

```bash
git switch main
git pull --ff-only
git tag -s v0.3.0 -m "qbvisor 0.3.0"
git push origin v0.3.0
```

On GitHub, draft a release for that existing tag. Review the generated notes against the changelog,
state compatibility concerns and known limitations, and then publish the GitHub Release. Publishing
the release starts the trusted-publishing workflow. Prereleases are intentionally ignored.

## Verify publication

Confirm that the Release workflow completed and that PyPI shows both distributions and their
attestations. Then install the exact version into an isolated environment:

```bash
uv run --isolated --no-project --with qbvisor==0.3.0 \
  python -c "import qbvisor; print(qbvisor.__version__)"
```

The printed version must match the tag. Verify the public project links and repeat the smallest
non-mutating Quickbase connection check before announcing the release.

## Failure and recovery

- If release identity or artifact validation fails, correct the source through a new pull request.
  Do not bypass the validation job.
- If trusted authentication fails, compare the owner, repository, workflow filename, and
  environment on GitHub and PyPI. After correcting configuration, rerun the failed job while its
  validated artifact is retained.
- Never move a release tag or attempt to overwrite a version that reached PyPI. Correct a published
  defect in the next patch release and document its impact.
- A release can be marked as yanked on PyPI when users should avoid it, but yanking is not a
  substitute for publishing a corrected version.
