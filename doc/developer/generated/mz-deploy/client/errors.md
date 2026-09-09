---
source: src/mz-deploy/src/client/errors.rs
revision: d50441fcbe
---

# mz-deploy::client::errors

Error types for the client module.

Two top-level enums cover different failure modes:

- `ConnectionError` — Transport and query failures: connection refused, SQL errors, missing dependencies, configuration problems, and DDL failures.
- `DatabaseValidationError` — Semantic mismatches detected during pre-deployment validation.

`DatabaseValidationError` includes a `MissingSourceReferences` variant backed by `SourceReferenceMismatch` and `MissingSourceReference`. `SourceReferenceMismatch` groups by source: it records the source's catalog ID, how many references it exposes, which tables asked for unresolvable references (`MissingSourceReference`), and an `unreadable` field that explains why the references could not be refreshed when the refresh step failed. `MissingSourceReference` carries the table ID, the reference as written, and fuzzy-matched suggestions sorted best-first.
