---
source: src/adapter/src/statement_logging.rs
revision: 07e0546b22
---

# adapter::statement_logging

Implements sampled statement execution logging to the built-in `mz_statement_execution_history` and `mz_prepared_statement_history` catalog tables.
`StatementBeganExecutionRecord` and related structs capture the full context of a statement at the start and end of execution; `StatementLifecycleEvent` enumerates the named milestones (optimization finished, storage/compute dependencies ready, execution finished) whose timestamps are tracked.
`StatementLoggingFrontend` provides the client-facing interface used by the frontend peek path to log statements without holding a coordinator lock.
The sampling logic uses a Bernoulli distribution parameterized by `statement_logging_sample_rate` from `SystemVars`, and records SHA-256 hashes to deduplicate repeated statement text.
