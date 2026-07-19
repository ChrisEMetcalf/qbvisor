# Security policy

qbvisor processes Quickbase credentials, application metadata, records, and file attachments. A
security report should be handled privately until a fix is available.

## Report a vulnerability

Email `christopher.e.metcalf@gmail.com` with the subject `[qbvisor security]`. Include:

- The affected qbvisor version or commit
- The expected and observed behavior
- Reproduction steps or a minimal proof of concept
- The potential effect on credentials, Quickbase data, or local files
- Any mitigation already available to users

Do not include active Quickbase tokens, production records, customer data, or sensitive
attachments. Replace those values with minimal test fixtures. Do not open a public GitHub issue for
an unpatched vulnerability.

Receipt and remediation timelines depend on severity and maintainer availability. The issue will be
evaluated, reproduced when possible, and coordinated with the reporter before public disclosure.

## Supported versions

Security fixes target the latest tagged release and the current `main` branch. Older releases may
require an upgrade rather than a backport. Until qbvisor reaches `1.0.0`, support guarantees and any
required migration will be stated in the release notes.

## Credential handling

- Supply Quickbase tokens through environment variables or another secret store. Never commit
  `.env` files.
- Scope development and CI tokens to dedicated sandbox applications.
- Treat backups, exported records, logs produced by consuming applications, and downloaded
  attachments according to the source application's data classification.
- Rotate a token immediately if it appears in a commit, issue, test output, log, or security report.
