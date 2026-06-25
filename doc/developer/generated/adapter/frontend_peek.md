---
source: src/adapter/src/frontend_peek.rs
revision: 7258dad07f
---

# adapter::frontend_peek

Implements the "frontend peek" sequencing path, where SELECT query optimization and fast-path execution are performed in the pgwire connection task rather than the coordinator's main loop.
`PeekClient::try_frontend_peek` is the main entry point; it verifies the portal, delegates statement logging setup to `PeekClient::begin_statement_logging` (which returns a `StatementLoggingGuard`), immediately defuses the guard, and then calls `try_frontend_peek_inner`.
`try_frontend_peek_inner` sequences the full pipeline — name resolution, planning, cluster selection, RBAC checks, timeline context determination, timestamp determination with read-hold acquisition, optimization (in a `spawn_blocking` task), and finally execution — handling `SELECT`, `EXPLAIN` (plan and pushdown), `COPY TO S3`, and `SUBSCRIBE` statement types.
Fast-path peeks (constant, arrangement, or persist) are executed directly via `implement_fast_path_peek_plan`; slow-path dataflow plans are dispatched to the coordinator via `Command::ExecuteSlowPathPeek`.
SUBSCRIBE statements are dispatched via `Command::ExecuteSubscribe`; COPY TO S3 performs an S3 preflight check via `Command::CopyToPreflight` before dispatching `Command::ExecuteCopyTo`.
The `ENABLE_FRONTEND_SUBSCRIBES` dyncfg gates the SUBSCRIBE frontend path specifically; the overall frontend peek sequencing is enabled by the `ENABLE_FRONTEND_PEEK_SEQUENCING` session/system variable checked at connection startup.
The optimizer config is built by layering cluster features, then cluster-coherent scoped overrides (`CatalogState::cluster_scoped_optimizer_overrides`), over the base `OptimizerConfig`. A cluster-scoped LaunchDarkly rule beats a manual `FEATURES` pin on the cluster, following the scoped feature flags resolution order.
The private `Execution` enum branches the post-optimization flow among `Peek`, `Subscribe`, `CopyToS3`, `ExplainPlan`, and `ExplainPushdown` variants.
`frontend_determine_timestamp` mirrors the coordinator's `determine_timestamp`, acquiring read holds and computing a `TimestampDetermination` entirely within the session task. For bounded-staleness queries that do not respond immediately, it also records a `timestamp_difference_for_bounded_staleness_ms` session metric comparing the chosen timestamp against what serializable would have produced.
