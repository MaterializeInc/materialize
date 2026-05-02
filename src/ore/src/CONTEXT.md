# ore::src

The implementation of `mz-ore`, Materialize's internal utility crate. ~50 independent
modules; no shared internal state; no module calls another (each is a standalone leaf).

## Modules (selected by significance or size)

| Module | LOC | What it provides |
|---|---|---|
| `metrics.rs` | 1053 | `MetricsRegistry`, `metric!` macro, `DeleteOnDropWrapper`, `MetricsFutureExt`; Prometheus adapter used by ~121 crates |
| `retry.rs` | 1032 | `Retry` builder with exponential backoff; sync and async `retry`/`retry_async` methods |
| `tracing.rs` | 1019 | `configure()` for OTel + tracing-subscriber setup; `OpenTelemetryContext` for span propagation across task boundaries; Sentry integration |
| `bytes.rs` | 898 | `MaybeLgBytes`, `SegmentedBytes` — arena-backed and segmented byte buffers |
| `overflowing.rs` | 766 | `Overflowing<T>` newtype for wrapping arithmetic |
| `netio/socket.rs` | 655 | Async socket abstractions (`MzTcpListener`, Unix socket helpers) |
| `id_gen.rs` | 558 | `IdAllocator`, `OrdIdGenerator`, `HumanReadableIdGen` — thread-safe sequential ID generators |
| `assert.rs` | 535 | Soft-assertion macros (`soft_assert_or_log`, `soft_panic_or_log`, etc.); runtime toggle via `MZ_SOFT_ASSERTIONS`; used in ~114 crates |
| `collections/hash.rs` | 523 | Deterministic `HashMap`/`HashSet` via `FnvHasher`; `BTreeMapExt`, `AssociativeExt` |
| `future.rs` | 517 | `OrExt` combinator, `TimeoutFuture`, `InTask` |
| `str.rs` | 508 | `StrExt`, `Indent`, `Separated`, string diff utilities |
| `region.rs` | 445 | `LgAllocRegion<T>` — region allocator with lgalloc backing; stable memory positions |
| `task.rs` | 431 | `spawn`/`spawn_blocking` named wrappers; `AbortOnDropHandle`; `RuntimeExt`; used in ~132 crates |
| `cast.rs` | 357 | `CastFrom`/`CastInto`/`TryCastFrom` traits — lossless casts replacing unsafe `as` |
| `now.rs` | 165 | `NowFn` — injectable clock (`Arc<dyn Fn() -> EpochMillis>`); `SYSTEM_TIME` / `NOW_ZERO` statics; used in ~87 crates |
| `lgbytes.rs` | 287 | `MetricsRegion<T>` — `Region<T>` with drop-time Prometheus counters; converts to `bytes::Bytes` |

## Key concepts

- **No feature-flag coupling**: always-available modules (`cast`, `collections`, `graph`, `now`, etc.) import nothing from feature-gated modules. The graph of intra-crate dependencies is a DAG of at most two levels (e.g., `lgbytes` → `region` + `metrics`).
- **`NowFn` as a seam**: `now::NowFn` is the canonical injection point for deterministic time across the entire codebase. Its `Arc<dyn Fn()>` design lets tests substitute a fixed clock without requiring trait objects on callers.
- **Soft assertions**: `assert.rs` implements a build-time/runtime toggle that logs instead of panicking in production; the `ctor` initializer reads `MZ_SOFT_ASSERTIONS` at process start.
- **`AbortOnDropHandle`**: `task::AbortOnDropHandle<T>` wraps Tokio `JoinHandle` and aborts the task on drop; this RAII pattern is the workspace convention for structured concurrency.
- **`MetricsRegistry`**: wraps Prometheus `Registry` to enforce per-subsystem metric structs registered exactly once; `DeleteOnDropWrapper` and `delete_on_drop` sub-module enforce RAII label management.

## Cross-references

- `mz-ore-proc` (sibling proc-macro crate): provides `instrument`, `static_list`, and `test` macros re-exported at the crate root.
- Nearly every workspace crate depends on `mz-ore`; downstream coupling is broad and shallow — callers import individual module items, not the whole crate.
- Generated docs: `doc/developer/generated/ore/`.
