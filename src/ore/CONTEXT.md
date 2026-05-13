# ore (mz-ore)

Materialize's internal utility ("standard library") crate. Modules are included here
when they are broadly useful but too small to warrant their own crate.

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

`Cargo.toml` — feature-gated design keeps the default build lightweight; heavy
deps (tokio, prometheus, opentelemetry, lgalloc) are optional features

## Architectural character

`mz-ore` is an intentional kitchen sink. There is no internal friction worth
surfacing: every module is a standalone leaf, there are no circular dependencies
within the crate, and the feature-flag partitioning cleanly separates always-available
primitives from async/metrics/tracing capabilities.

## What bubbles up to src/CONTEXT.md

- **`NowFn` (now.rs)** — the workspace-wide injectable clock seam. Used by ~87
  crates. Any future time-related abstraction should extend `NowFn` rather than
  introduce a parallel clock type.
- **`AbortOnDropHandle` (task.rs)** — workspace convention for structured
  concurrency with Tokio. Used by ~132 crates. The pattern is stable and pervasive.
- **`soft_assert_*` macros (assert.rs)** — workspace-wide production-safe assertion
  pattern. Used by ~114 crates. The `MZ_SOFT_ASSERTIONS` toggle is an operational
  knob that SRE/on-call should know about.
- **`MetricsRegistry` (metrics.rs)** — the single Prometheus registry wrapper for
  the entire workspace. ~121 crates depend on it. The `DeleteOnDropWrapper` pattern
  is the workspace standard for metric label lifecycle.
- **Feature-flag discipline**: `mz-ore`'s Cargo feature design is a model for
  keeping utility crates lean. The always-on surface (`cast`, `collections`, `iter`,
  `now`, `graph`, etc.) is zero-cost for WASM or minimal builds.

## Cross-references

- `mz-ore-proc` (`src/ore-proc/`) — proc macros re-exported by `mz-ore`
- Generated docs: `doc/developer/generated/ore/_crate.md`
