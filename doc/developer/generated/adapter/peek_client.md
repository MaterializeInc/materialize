---
source: src/adapter/src/peek_client.rs
revision: dbd2c3fc06
---

# adapter::peek_client

Provides `PeekClient`, which bundles a `Client` handle with a session-side catalog snapshot cache (`catalog_cache: Weak<Catalog>`), lazily-populated direct channels to each compute instance (`compute_instances`), lazily-populated per-timeline timestamp oracles (`oracles`), a `PersistClient`, storage-collection frontier access, a transient ID generator, optimizer metrics, and a `StatementLoggingFrontend`.
This allows the frontend peek sequencing path (and other callers) to issue peeks directly to compute instances and check persist fast-path conditions without serialising through the coordinator's main event loop.
`PeekClient::catalog_snapshot` serves the catalog from the session-side `catalog_cache` when the catalog's transient revision is unchanged (verified via `Catalog::transient_revision_is_current`), avoiding a coordinator round-trip on cache hits. On a miss it sends `Command::CatalogSnapshot`, re-populates `catalog_cache` with a `Weak` downgrade of the returned `Arc`, and records the round-trip latency in `mz_catalog_snapshot_seconds` (labeled by `context`). Both hits and misses increment `mz_catalog_snapshot_cache` (labeled `context` and `result`). The `catalog_cache` holds a `Weak` so that an idle session does not prevent an old catalog version from being freed.
`PeekClient::new` seeds `catalog_cache` from the `Arc<Catalog>` returned by `Command::Startup`, so the session's first statements do not require a round-trip.
`CollectionLookupError` enumerates errors that can occur when consulting storage or compute frontiers for timestamp selection.
`begin_statement_logging` sets up statement logging for a frontend-sequenced operation: it begins a new execution log entry for fresh statements, or inherits and retires the outer context for nested executions (e.g., EXECUTE/FETCH), and returns a `StatementLoggingGuard`.
`StatementLoggingGuard` is an RAII guard that ensures every statement for which `BeganExecution` was logged also receives a corresponding `EndedExecution`: if dropped without being defused or retired, it emits `StatementEndedExecutionReason::Aborted`. Callers call `retire` to log a terminal outcome explicitly, or `defuse` (taking `&mut self`) to hand off logging responsibility to another component (e.g. the coordinator for streaming peek responses) at the exact handoff point; afterwards the guard is inert. For non-sampled statements the guard carries no id and is a no-op.
