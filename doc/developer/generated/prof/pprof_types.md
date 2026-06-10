---
source: src/prof/src/pprof_types.rs
revision: a86b5041c1
---

# mz-prof::pprof_types

Thin generated-code shim that includes the protobuf-generated `perftools.profiles` Rust bindings at compile time via `include!`.
Used internally by `lib.rs` to encode stack profiles into the pprof wire format.
