---
source: src/build-info/src/lib.rs
revision: 2c0a6826d4
---

# mz-build-info

Provides a single, dependency-free `BuildInfo` struct that carries the version string and Git SHA of a Materialize build, together with supporting macros and a compile-time target triple constant.

## Module structure

The crate has no submodules.
All public items live directly in `lib.rs`:

* `BuildInfo` — the core struct, holding `version: &'static str` and `sha: &'static str`, with methods for human-readable formatting (`human_version`), semver parsing (`semver_version`, `semver_version_build`, `version_num`, all gated on the `semver` feature), and a dev-build check (`is_dev`).
* `DUMMY_BUILD_INFO` — a sentinel constant for tests and other contexts where real build metadata is unavailable.
* `TARGET_TRIPLE` — a `&'static str` populated at compile time via `env!("TARGET_TRIPLE")`.
* `build_info!` macro — the primary entry point; intended to be called once in the final binary crate to capture `CARGO_PKG_VERSION` and the current Git SHA at compile time.
* `__git_sha_internal!` / `private` module — implementation details of the macro; resolves the Git SHA from `$MZ_DEV_BUILD_SHA` or `git rev-parse HEAD` at compile time using `compile-time-run`.

## Key dependencies

* `compile-time-run` — executes a shell command at compile time to capture the Git SHA.
* `buildid` — provides the platform build ID used in `semver_version_build`.
* `hex` — encodes the raw build ID bytes for inclusion in semver build metadata.
* `semver` (optional, default-enabled) — parses and exposes the version as a structured `semver::Version`.

## Downstream consumers

The crate is intended to be a leaf in the dependency graph from the consumer's perspective: binaries invoke `build_info!()` once and pass the resulting `BuildInfo` down into libraries that accept it.
This keeps build metadata accessible throughout the stack without pulling heavyweight dependencies into lower layers.
