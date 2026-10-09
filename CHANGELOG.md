# Changelog

## 0.1.2 — 2026-10-09

- Attribute the MIT copyright to braz and name QueueLite in the README license section.
- Documentation and license attribution only; runtime behavior is unchanged.

## 0.1.1 — 2026-10-09

Initial experimental public release after cross-platform validation fixes.

- Skip dependency lifecycle scripts in locked development installs to avoid an
  unnecessary native rebuild on Windows; keep normal installation in consumer checks.
- Use npm 11.6.4 as the cross-platform CI baseline.
- Hold the concurrency scenario's workload until its monotonic deadline, keeping
  the two-second minimum assertion even when a platform timer wakes early.
- Retain the actual failing operational scenario in nested-runner diagnostics.
- Allow the multi-batch recovery test to finish on slower CI disks without
  reducing its job count or recovery assertions.
- Keep the initial `v0.1.0` preparation tag; no package was published at that version.

## 0.1.0 — 2026-10-09

Initial experimental release candidate, prepared for publication as
`@thebraz/queuelite`. Publication status is verified against npm and GitHub
separately; this entry alone is not proof of publication.

- Typed ESM SDK and `queuelite` CLI for a local persistent SQLite queue.
- Atomic claims, per-worker concurrency, priorities, delayed jobs, fixed and
  exponential retries with jitter, persistent idempotency keys and renewable
  leases with bounded crash recovery.
- Graceful shutdown, cooperative cancellation, manual retry, pending-job
  cancellation, paginated inspection, attempt history, statistics, safe lifecycle
  events and non-mutating diagnostics.
- Reject custom array prototypes before serialization to prevent inherited
  serializers from executing and replacing validated payloads.
- Public package metadata, MIT license, clean tarball allowlist, consumer
  installation checks and GitHub Actions validation.

Processing can repeat external effects. Handlers must be idempotent; retries
have a finite attempt budget and successful delivery is not guaranteed. Local
same-host files only; network filesystems and distributed multi-host operation
are unsupported. SQLite serializes writes. Native installation and workload
behavior need validation on the deployment platform. See the README for schema
migration, retention, shutdown and security limits.
