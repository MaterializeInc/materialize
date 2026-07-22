---
source: src/adapter/src/coord/statement_logging.rs
revision: 770c31e9f8
---

# adapter::coord::statement_logging

Implements the coordinator-side statement logging: `WatchSet` registration and resolution (triggering log writes once storage and compute frontiers advance past the execution timestamp), and the actual writes to `mz_statement_execution_history` and `mz_prepared_statement_history`.
`handle_statement_logging_watch_set` is called from the message handler when watched frontiers advance, completing deferred statement log entries.
`end_statement_execution` is idempotent: the first end wins, and later duplicate ends for the same statement are ignored with a `tracing::warn!`. Duplicate ends are legitimate under async cancellation: ownership of the end-of-execution log is handed from the frontend to the coordinator at dispatch time, and a client disconnect can drop the frontend future after the coordinator registers a peek but before the frontend defuses its logging guard, leaving both sides holding end ownership.
`PreparedStatementLoggingInfo::AlreadyLogged` carries both the `uuid` and the statement `kind`, so that `end_statement_execution` can detect secret statements (`kind.is_secret()`) and replace the error message with a fixed redaction string rather than persisting any error text that may embed secret material. The `kind` is captured from the session at `begin_statement_execution` time via `session.qcell_ro(logging).kind()` and passed through `create_began_execution_record`.
