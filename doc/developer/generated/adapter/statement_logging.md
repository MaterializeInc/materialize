---
source: src/adapter/src/statement_logging.rs
revision: dd294d285b
---

# adapter::statement_logging

Implements sampled statement execution logging to the built-in `mz_statement_execution_history` and `mz_prepared_statement_history` catalog tables.
`StatementBeganExecutionRecord` and related structs capture the full context of a statement at the start and end of execution; `StatementLifecycleEvent` enumerates the named milestones (optimization finished, storage/compute dependencies ready, execution finished) whose timestamps are tracked.
`StatementLoggingFrontend` provides the client-facing interface used by the frontend peek path to log statements without holding a coordinator lock.
`get_prepared_statement_info` is a read-only operation: it takes `&Session` and `&Arc<QCell<PreparedStatementLoggingInfo>>`, builds the prepared-statement event rows if the statement has not yet been logged, and returns them alongside the statement UUID. It does not mutate the `PreparedStatementLoggingInfo` metadata. `record_prepared_statement_as_logged` is the separate method that transitions the metadata from `StillToLog` to `AlreadyLogged`; it is called only after the sampling and throttling checks in `begin_statement_execution` have passed.
The sampling logic uses a Bernoulli distribution parameterized by `statement_logging_sample_rate` from `SystemVars`, and records SHA-256 hashes to deduplicate repeated statement text.
