# ore (mz-ore)

Materialize's internal utility ("standard library") crate. Modules are included here
when they are broadly useful but too small to warrant their own crate.

## Structure

- `src/` — all implementation; see [`src/CONTEXT.md`](src/CONTEXT.md)
- `Cargo.toml` — feature-gated design keeps the default build lightweight; heavy
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
