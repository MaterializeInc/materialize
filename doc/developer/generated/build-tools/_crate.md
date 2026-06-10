---
source: src/build-tools/src/lib.rs
revision: 44c64a9e5a
---

# mz-build-tools

Provides access to build-time tools — primarily `protoc` and its include directory — for use in build scripts across the Materialize workspace.

## Module structure

The crate is a single `lib.rs` with no submodules.
It exposes two public functions: `protoc()` and `protoc_include()`.

## Key dependencies

* `protobuf-src` (optional, default): bootstraps a vendored `protoc` binary at build time.
* `which`: falls back to locating `protoc` on the system `$PATH` when `protobuf-src` is not enabled.
* `cfg-if`: used to select the resolution strategy at compile time based on active features.

## Features

* `default` / `protobuf-src`: uses the vendored protobuf-src crate to supply `protoc`.
* `bazel`: enables Bazel-aware behavior; under Bazel, `protoc` and the well-known-type protos are provided as Bazel data dependencies rather than being bootstrapped by `protobuf-src`.

## Downstream consumers

Used by build scripts (`build.rs`) throughout the workspace that need to invoke `protoc` to compile `.proto` files.
