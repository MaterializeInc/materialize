---
source: src/adapter/src/coord/sequencer/inner.rs
revision: e7ac38b338
---

# adapter::coord::sequencer::inner

Implements the majority of `sequence_*` methods: general DDL (CREATE/DROP/ALTER for sources, sinks, connections, tables, types, roles, schemas, databases, network policies), DML (INSERT, UPDATE, DELETE via `sequence_read_then_write`), transaction management, cursor operations, COPY FROM, SHOW, SET/RESET, FETCH, RAISE, and more.
Each method validates the plan against the current catalog and session state, performs the catalog transaction, applies catalog implications, and returns the appropriate `ExecuteResponse`.
The `inner` sub-modules split out the most complex individual statement types (peek, subscribe, cluster, index, materialized view, view, copy_from, secret, explain_timestamp) into separate files.
Also defines the `sequence_staged` generic driver and `Staged` / `StagedContext` traits used across all multi-stage sequencing pipelines.
`validate_role_attributes` permits the `LOGIN` attribute even when password auth is disabled, restricting the unavailable-feature gate to `SUPERUSER` and `PASSWORD` attributes.
`CREATE CONNECTION ... VALIDATE` and `ALTER CONNECTION ... VALIDATE` tasks are wrapped in `ore_catch_unwind` to convert panics (e.g., from malformed TLS material) into `AdapterError::Internal` rather than crashing the coordinator.
