---
source: src/adapter/src/statement_logging.rs
revision: 7d5791b137
---

# adapter::statement_logging

Implements sampled statement execution logging to the built-in `mz_statement_execution_history` and `mz_prepared_statement_history` catalog tables.
`StatementBeganExecutionRecord` and related structs capture the full context of a statement at the start and end of execution; the record includes an optional `kind: Option<StatementKind>` field used to redact `error_message` for statement kinds that can carry secret material. `StatementLifecycleEvent` enumerates the named milestones (optimization finished, storage/compute dependencies ready, execution finished) whose timestamps are tracked.
`StatementLoggingFrontend` provides the client-facing interface used by the frontend peek path to log statements without holding a coordinator lock.
`PreparedStatementLoggingInfo::AlreadyLogged` carries a `kind: Option<StatementKind>` field alongside the `uuid`, so statement kind is available regardless of logging state. `PreparedStatementLoggingInfo::kind()` retrieves it uniformly from either variant. SQL text for sensitive statement kinds (those where `StatementKind::is_sensitive()` returns true, such as `CREATE SECRET`, `ALTER SECRET`, `INSERT`, `UPDATE`, and `EXECUTE`) is redacted. `get_prepared_statement_info` is a read-only operation: it takes `&Session` and `&Arc<QCell<PreparedStatementLoggingInfo>>`, builds the prepared-statement event rows if the statement has not yet been logged, and returns them alongside the statement UUID. It does not mutate the `PreparedStatementLoggingInfo` metadata. `record_prepared_statement_as_logged` is the separate method that transitions the metadata from `StillToLog` to `AlreadyLogged`; it is called only after the sampling and throttling checks in `begin_statement_execution` have passed.
The sampling logic uses a Bernoulli distribution parameterized by `statement_logging_sample_rate` from `SystemVars`, and records SHA-256 hashes to deduplicate repeated statement text.
