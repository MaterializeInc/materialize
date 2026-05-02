# adapter::src

The `mz-adapter` crate root: the coordinator and all subsystems it depends on.
Approximately 70,668 LOC across `coord/`, `catalog/`, `optimize/`, `explain/`,
and a set of top-level modules.

## Subsystem map

| Subsystem | LOC | What it owns |
|---|---|---|
| `coord/` | 28,576 | `Coordinator` struct, event loop, all sub-modules; see [`coord/CONTEXT.md`](coord/CONTEXT.md) |
| `catalog/` | 16,458 | Adapter catalog layer over `mz_catalog`; see [`catalog/CONTEXT.md`](catalog/CONTEXT.md) |
| `catalog.rs` | 3,691 | `Catalog` facade struct (116 pub methods, most delegating to `CatalogState`) |
| `coord.rs` | 5,179 | `Coordinator` struct definition, `Message` enum, `serve`/`Config` entry points |
| `optimize/` | 2,731 | `Optimize<From>` trait + per-statement optimizer pipelines (peek, index, MV, subscribe, COPY TO, view); `DataflowBuilder` |
| `session.rs` | 1,845 | `Session` per-connection state, `TransactionStatus`, `PreparedStatement`, `RowBatchStream` |
| `explain/` | 1,116 | `EXPLAIN` support across all IR stages; `OptimizerTrace` capture |
| `frontend_peek.rs` | 1,745 | Frontend peek coordination (slow-path query execution off the coordinator) |
| `client.rs` | 1,379 | `Client`, `SessionClient`, `Handle` — the public external API consumed by pgwire |
| `statement_logging.rs` | 1,020 | Statement execution sampling and persistence for `mz_recent_activity_log` |
| `error.rs` | 1,302 | `AdapterError` — the crate's unified error type |
| `active_compute_sink.rs` | 462 | `ActiveComputeSink` — tracks in-progress SUBSCRIBE/COPY TO sinks |
| `command.rs` | 960 | `Command` enum — all external commands sent to the coordinator |
| `webhook.rs` | 458 | Webhook appender invalidation handle |
| `session.rs` | (above) | |
| `util.rs` | 545 | Shared utilities |
| `peek_client.rs` | 544 | `PeekClient` — executes peeks from `SessionClient` against the coordinator |
| `metrics.rs` | 358 | Coordinator Prometheus metrics |
| `notice.rs` | 517 | `AdapterNotice` — advisory messages returned to clients |
| `config/` | 621 | System-parameter sync with LaunchDarkly or JSON file |
| `telemetry.rs` | 172 | Segment telemetry helpers |
| `flags.rs` | 211 | Feature flags derived from system configuration |
| `continual_task.rs` | 78 | `ContinualTask` helpers |

## Key concepts

- **`serve`** — the crate's main entry point (called by `mz-environmentd`). Initializes the catalog, bootstraps the coordinator, then runs the single-threaded event loop.
- **Three-layer write path** — client `Command` → `handle_command` (sequencing) → `Catalog::transact` (durable) → `apply_catalog_implications` (controller mutations). Each layer has a clear, non-overlapping responsibility.
- **`Optimize<From>` trait** — the codebase's seam between planning and dataflow construction. Each statement type has a dedicated pipeline (`optimize/peek.rs`, `optimize/index.rs`, etc.) that transforms stage-result structs through a sequence of `Optimize` impls. The optimizer never touches the `Coordinator` directly; it receives an `OptimizerCatalog` view.
- **Front-end offload** — several `Command` variants (`ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `RegisterFrontendPeek`) route slow-path query execution outside the coordinator's main loop, reducing head-of-line blocking. `frontend_peek.rs` coordinates this.
- **`AdapterError`** — single unified error type for all layers; avoids error-type proliferation across subsystems.

## Architecture notes

- The separation between `catalog.rs` (3,691 LOC facade) and `catalog/` (16,458 LOC implementation) follows the pattern of the rest of the codebase: a top-level `.rs` file is the module declaration and public API; a same-named directory holds the implementation files.
- `optimize/` is a true seam: it has no `Coordinator` dependency and can be exercised independently. The optimizer receives `OptimizerCatalog` (a minimal trait) and `OptimizerConfig` (flags), not a `Coordinator` reference.
- `explain/` wraps every optimizer stage for `EXPLAIN` output; `optimizer_trace.rs` captures the full trace via a thread-local, not by modifying the optimizer pipelines.
- No circular dependencies observed: `catalog` → `mz_catalog`; `coord` → `catalog` + `optimize`; `client` → `coord` (via channel only).

## Bubbled from coord/

- **`sequencer/ARCH_REVIEW.md`** — parallel `Staged` match arms in each stage enum (leaf finding).
- **`sequencer/ARCH_REVIEW.md`** — 9 per-statement `Message::*StageReady` variants in `coord.rs`, all dispatched identically in `message_handler.rs`; adding a new staged statement type is a 3-file change.

## Bubbled from catalog/

- Catalog write path spans two crates cleanly (`mz_catalog` durable store + `adapter::catalog` transaction logic); no friction observed at the interface.
- `apply.rs` + `transact.rs` are large by coverage (every catalog object type), not by structural complexity.

## Cross-references

- Entry: `mz-environmentd` → `adapter::serve`.
- Consumers: `mz-pgwire` (uses `Client`/`SessionClient`), `mz-balancerd`.
- Below: `mz-controller`, `mz-compute-client`, `mz-storage-client`, `mz-sql`, `mz-expr`, `mz-transform`, `mz-persist-client`, `mz-timestamp-oracle`.
- Generated developer docs: `doc/developer/generated/adapter/`.
