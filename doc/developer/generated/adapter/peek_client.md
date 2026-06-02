---
source: src/adapter/src/peek_client.rs
revision: d109855c3d
---

# adapter::peek_client

Provides `PeekClient`, which bundles a `Client` handle with lazily-populated direct channels to each compute instance (`compute_instances`), lazily-populated per-timeline timestamp oracles (`oracles`), a `PersistClient`, storage-collection frontier access, a transient ID generator, optimizer metrics, and a `StatementLoggingFrontend`.
This allows the frontend peek sequencing path (and other callers) to issue peeks directly to compute instances and check persist fast-path conditions without serialising through the coordinator's main event loop.
`CollectionLookupError` enumerates errors that can occur when consulting storage or compute frontiers for timestamp selection.
`begin_statement_logging` sets up statement logging for a frontend-sequenced operation: it begins a new execution log entry for fresh statements, or inherits and retires the outer context for nested executions (e.g., EXECUTE/FETCH), and returns a `StatementLoggingGuard`.
`StatementLoggingGuard` is an RAII guard that ensures every statement for which `BeganExecution` was logged also receives a corresponding `EndedExecution`: if dropped without being defused, it emits `StatementEndedExecutionReason::Aborted`. Callers call `defuse` to hand off logging responsibility to another component (e.g. the coordinator for streaming peek responses). For non-sampled statements the guard carries no id and is a no-op.
