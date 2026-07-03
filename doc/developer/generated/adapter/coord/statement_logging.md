---
source: src/adapter/src/coord/statement_logging.rs
revision: 7d5791b137
---

# adapter::coord::statement_logging

Implements the coordinator-side statement logging: `WatchSet` registration and resolution (triggering log writes once storage and compute frontiers advance past the execution timestamp), and the actual writes to `mz_statement_execution_history` and `mz_prepared_statement_history`.
`handle_statement_logging_watch_set` is called from the message handler when watched frontiers advance, completing deferred statement log entries.
`end_statement_execution` uses `soft_panic_or_log!` to handle duplicate calls: if a `StatementLoggingId` is ended a second time the duplicate is reported loudly (so it surfaces in logs and tests) but ignored, keeping the first end record rather than crashing the coordinator.
`PreparedStatementLoggingInfo::AlreadyLogged` carries both the `uuid` and the statement `kind`, so that `end_statement_execution` can detect secret statements (`kind.is_secret()`) and replace the error message with a fixed redaction string rather than persisting any error text that may embed secret material. The `kind` is captured from the session at `begin_statement_execution` time via `session.qcell_ro(logging).kind()` and passed through `create_began_execution_record`.
