---
source: src/adapter/src/coord/statement_logging.rs
revision: 8f6821e6f4
---

# adapter::coord::statement_logging

Implements the coordinator-side statement logging: `WatchSet` registration and resolution (triggering log writes once storage and compute frontiers advance past the execution timestamp), and the actual writes to `mz_statement_execution_history` and `mz_prepared_statement_history`.
`handle_statement_logging_watch_set` is called from the message handler when watched frontiers advance, completing deferred statement log entries.
`end_statement_execution` uses `soft_panic_or_log!` to handle duplicate calls: if a `StatementLoggingId` is ended a second time the duplicate is reported loudly (so it surfaces in logs and tests) but ignored, keeping the first end record rather than crashing the coordinator.
