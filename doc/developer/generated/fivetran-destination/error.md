---
source: src/fivetran-destination/src/error.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::error

Defines `OpError` and `OpErrorKind`, the error types used throughout the Fivetran destination connector.
`OpErrorKind` enumerates all failure modes — Postgres errors, CSV mapping failures, filesystem errors, missing fields, privilege errors, and more — and classifies which are retryable via `can_retry`.
The `Context` trait provides chainable context annotation, mirroring the `anyhow::Context` pattern.
