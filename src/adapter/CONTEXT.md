# adapter (mz-adapter crate)

The `mz-adapter` crate is Materialize's coordinator: the central component that
receives SQL commands from clients, plans and optimizes them, assigns timestamps,
drives catalog transactions, and dispatches work to the storage and compute layers.
Approximately 71,323 LOC (Rust source + tests).

## Structure

```
src/adapter/
  src/                  # crate source root; see src/CONTEXT.md
    coord/              # Coordinator struct + all sub-modules (~28,576 LOC)
    catalog/            # Adapter catalog layer (~16,458 LOC)
    optimize/           # Optimizer pipelines (~2,731 LOC)
    explain/            # EXPLAIN support (~1,116 LOC)
    config/             # System-param sync (~621 LOC)
    *.rs                # Top-level modules (~21,166 LOC)
  tests/                # Integration tests
  benches/              # Benchmarks
```

## Key public surface

- **`serve(Config) -> Handle`** — starts the coordinator; called by `mz-environmentd`.
- **`Client` / `SessionClient`** — consumed by `mz-pgwire` for all per-connection SQL operations.
- **`ExecuteResponse`** — the coordinator's per-statement result type.
- **`AdapterError`** — unified error type.
- **`CollectionIdBundle`, `ReadHolds`, `TimestampContext`** — re-exported for downstream consumers.

## Subsystem summary

| Subsystem | LOC | Role |
|---|---|---|
| `coord` | 28,576 | `Coordinator` struct, event loop, all sub-modules; see [`src/coord/CONTEXT.md`](src/coord/CONTEXT.md) |
| `catalog` | 16,458 | Adapter catalog layer over `mz_catalog`; see [`src/catalog/CONTEXT.md`](src/catalog/CONTEXT.md) |
| `catalog.rs` | 3,691 | `Catalog` facade struct (116 pub methods, most delegating to `CatalogState`) |
| `coord.rs` | 5,179 | `Coordinator` struct definition, `Message` enum, `serve`/`Config` entry points |
| `optimize` | 2,731 | `Optimize<From>` trait + per-statement optimizer pipelines (peek, index, MV, subscribe, COPY TO, view); `DataflowBuilder` |
| `session.rs` | 1,845 | `Session` per-connection state, `TransactionStatus`, `PreparedStatement`, `RowBatchStream` |
| `explain` | 1,116 | `EXPLAIN` support across all IR stages; `OptimizerTrace` capture |
| `frontend_peek.rs` | 1,745 | Frontend peek coordination (slow-path query execution off the coordinator) |
| `client.rs` | 1,379 | `Client`, `SessionClient`, `Handle` — the public external API consumed by pgwire |
| `statement_logging.rs` | 1,020 | Statement execution sampling and persistence for `mz_recent_activity_log` |
| `error.rs` | 1,302 | `AdapterError` — the crate's unified error type |
| `active_compute_sink.rs` | 462 | `ActiveComputeSink` — tracks in-progress SUBSCRIBE/COPY TO sinks |
| `command.rs` | 960 | `Command` enum — all external commands sent to the coordinator |
| `webhook.rs` | 458 | Webhook appender invalidation handle |
| `util.rs` | 545 | Shared utilities |
| `peek_client.rs` | 544 | `PeekClient` — executes peeks from `SessionClient` against the coordinator |
| `metrics.rs` | 358 | Coordinator Prometheus metrics |
| `notice.rs` | 517 | `AdapterNotice` — advisory messages returned to clients |
| `config` | 621 | System-parameter sync with LaunchDarkly or JSON file |
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

## Architectural observations (from inner review)

### Verified findings worth surfacing to `src/`

1. **`Message` enum grows with staged statement types** (`src/adapter/src/coord.rs:363-406`).
   Each new staged statement type (peek, subscribe, create index, etc.) requires a new
   `Message::XxxStageReady` variant in `coord.rs` and a new (structurally identical)
   match arm in `message_handler.rs`. Currently 9 such variants; all 9 dispatch identically
   (`self.sequence_staged(ctx, span, stage)`). Collapsing to a single polymorphic
   `Message::StagedReady(Box<dyn Staged>)` would remove this per-type coupling.
   See `src/coord/sequencer/ARCH_REVIEW.md`.

2. **Parallel match arms in `Staged` impls** (`src/adapter/src/coord/sequencer/inner/peek.rs` etc.).
   Each stage enum (`PeekStage`, `SubscribeStage`, etc.) has two parallel match-on-all-variants
   methods (`validity()` and `stage()`). Adding or renaming a variant requires lockstep edits
   in both. See `src/coord/sequencer/inner/ARCH_REVIEW.md`.

### Honest skips

- Catalog dual-layer (`mz_catalog` durable + `adapter::catalog` transact/apply): boundary is clean, no friction.
- `apply_catalog_implications` size (2,090 LOC): large by coverage (one case per catalog object type), not by structural complexity; deletion test fails.
- `optimize/` independence from `Coordinator`: a strength, not a friction.

## Key dependencies

`mz-catalog`, `mz-controller`, `mz-compute-client`, `mz-storage-client`, `mz-sql`,
`mz-expr`, `mz-transform`, `mz-persist-client`, `mz-timestamp-oracle`.

## Downstream consumers

`mz-environmentd` (calls `serve`), `mz-pgwire` (uses `Client`/`SessionClient`), `mz-balancerd`.

## Cross-references

- Coord detail: [`src/coord/CONTEXT.md`](src/coord/CONTEXT.md)
- Catalog detail: [`src/catalog/CONTEXT.md`](src/catalog/CONTEXT.md)
- Generated developer docs: `doc/developer/generated/adapter/_crate.md`
