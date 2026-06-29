---
source: src/adapter/src/flags.rs
revision: 6c2b81feaf
---

# adapter::flags

Provides helper functions that translate `SystemVars` into typed configuration structs consumed by other subsystems.
`compute_config`, `storage_config`, `tracing_config`, `caching_config`, `timestamp_oracle_config`, and `orchestrator_scheduling_config` each read the relevant system variables and build the corresponding parameter struct (`ComputeParameters`, `StorageParameters`, etc.).
This keeps knowledge of system variable names isolated to this module, so callers never need to inspect `SystemVars` directly.
`timestamp_oracle_config` includes `pg_statement_timeout`, populated from the `PG_TIMESTAMP_ORACLE_STATEMENT_TIMEOUT` dyncfg, which sets a server-side statement timeout on the Postgres/CRDB connections used by the timestamp oracle. A value of zero leaves the timeout unset.
