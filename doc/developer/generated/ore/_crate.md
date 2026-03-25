---
source: src/ore/src/lib.rs
revision: c9d4078b10
---

# mz-ore

Internal utility library for Materialize — "ore" as in the raw material from which more valuable materials are extracted.
Modules are included here when they are broadly useful but too small to warrant their own crate.

## Module structure

### Always-available modules (no feature gate)

| Module | Purpose |
|--------|---------|
| `bits` | Bit-manipulation helpers |
| `cast` | Checked, lossless numeric casts via the `CastFrom`/`CastInto`/`TryCastFrom` traits |
| `collections` | Extensions to std collections; `HashMap`/`HashSet` re-exports using a deterministic hasher; `AssociativeExt` trait |
| `env` | Environment variable helpers |
| `error` | `ErrorExt` trait (display with causes) and `ShouldTerminateProcess` |
| `fmt` | Formatting utilities: `FormatBuffer`, `Separated`, `bracketed`, `closure_to_display` |
| `graph` | Topological sort and transitive reduction for directed graphs |
| `hash` | Deterministic hashing setup |
| `hint` | `black_box` and `likely`/`unlikely` branch hints |
| `iter` | Iterator extensions and adapters |
| `lex` | Lexer helpers for SQL-related parsing |
| `now` | `NowFn` — injectable clock for deterministic timestamps |
| `num` | Numeric formatting helpers |
| `option` | `OptionExt` trait |
| `path` | `PathExt` |
| `permutations` | Permutation iterators |
| `result` | `ResultExt` trait |
| `serde` | Serde helpers for alternate serialization formats |
| `stats` | Simple stats (min/max/histogram) |
| `str` | String utilities: `StrExt`, indentation, separators |
| `thread` | Thread-spawning wrappers that capture tracing context |
| `time` | Duration/DateTime extensions |
| `treat_as_equal` | `TreatAsEqual` newtype wrapper for loosened equality semantics |
| `url` | `SensitiveUrl` — a URL wrapper that redacts passwords in Display |
| `vec` | `VecExt` and `BumpVec` |

### Feature-gated modules

| Module | Feature | Purpose |
|--------|---------|---------|
| `assert` | `assert` | Runtime assertion macros (`soft_assert`, `soft_panic_or_log`) that log instead of panicking in release builds |
| `bytes` | `bytes` | `MaybeLgBytes`, `SegmentedBytes`: arena-backed and segmented byte buffers |
| `channel` | `async` | `mpsc` wrappers with instrumentation; `trigger` submodule for one-shot trigger channels |
| `cli` | `cli` | Clap integration helpers |
| `future` | `async` | `OrExt` combinator, `TimeoutFuture`, `InTask` wrapper |
| `id_gen` | `id_gen` | Thread-safe sequential ID generators, including a `HumanReadableIdGen` |
| `lgbytes` | `bytes` + `region` + `tracing` | `LgBytes` backed by lgalloc, with metrics |
| `metrics` | `metrics` | Prometheus integration: `MetricsRegistry`, `metric!` macro, delete-on-drop wrappers, `MetricsFutureExt` |
| `netio` | `network` | Async networking utilities: framed codec, DNS, socket helpers, timeouts |
| `overflowing` | `overflowing` | `Overflowing<T>` newtype for wrapping arithmetic |
| `panic` | `panic` | Panic handler with Sentry integration |
| `process` | `process` | `halting` process exit via `libc` |
| `region` | `region` | Columnar region allocator with lgalloc support |
| `retry` | `async` | Configurable retry with exponential backoff |
| `secure` | `secure` | `SecureString`, `SecureVec`, and `zeroize` re-exports for sensitive data that must be zeroed from memory on drop |
| `stack` | `stack` | `maybe_grow` — checked stack growth via `stacker` |
| `task` | `async` | Tokio task/runtime spawning wrappers with tracing context propagation |
| `test` | `test` | Test helpers: `timeout` wrappers |
| `tracing` | `tracing` | OpenTelemetry + `tracing-subscriber` setup, Sentry layer, `stderr`/`json` log formatting |

### Re-exports from `mz-ore-proc`

`instrument`, `static_list`, `test` procedural macros are re-exported at the crate root.

## Key dependencies

* `mz-ore-proc` — the only internal dependency (proc macros)
* `either`, `itertools`, `serde`, `thiserror`, `derivative`, `paste`, `pin-project` — always linked
* All other dependencies are optional and feature-gated (see `Cargo.toml`)

## Downstream consumers

Nearly every crate in the Materialize workspace depends on `mz-ore`.
It is intentionally kept lightweight in its default feature set, with heavy dependencies (tokio, prometheus, opentelemetry, etc.) behind feature flags.
