# Contributing

Use Node.js 22.14 or newer and npm 11.6.4 (the CI baseline). Clone the repository, run `npm ci --ignore-scripts`, and run
`npm run validate` before submitting a pull request. `npm run verify:stage3`
also runs the full operational suite, including process and crash recovery scenarios.
These runners retain isolated temporary databases for inspection.

Keep changes focused. Include a regression test for changed state transitions,
retry behavior, serialization or public contracts. Do not change the SQLite
schema without an atomic migration and preservation/rollback tests. Changes to
delivery guarantees, defaults or public exports must update the README.

Report a reproducible bug with the Node.js version, operating system, package
version and a minimal example. Remove payloads, credentials and private database
paths from reports. For security issues, use the repository's private GitHub
security reporting channel when available; do not post credentials or sensitive
queue data in public issues.

Version 0.1.1 is an initial experimental release. No stable production or
throughput guarantee is offered. Maintainers review compatibility and release
versions explicitly; CI does not publish packages automatically.
