---
source: src/lowertest/src/lib.rs
revision: ddc1ff8d2d
---

# mz-lowertest

Provides a human-readable token-stream syntax for constructing and round-tripping Rust structs and enums in datadriven tests, targeting lower layers of the Materialize stack.

The crate defines the `MzReflect` trait (implemented via `mz-lowertest-derive`) and `ReflectedTypeInfo`, which together record field names and types at compile time so the runtime can convert a compact token-stream specification into serde-compatible JSON and then deserialize it into the target type.
The `to_json` / `from_json` pair implements bidirectional conversion, and the `TestDeserializeContext` trait lets callers override or extend the default syntax for domain-specific types.
Convenience wrappers `deserialize`, `deserialize_optional`, `serialize`, and their `_generic` variants cover the common case where no syntax extensions are needed.

Key dependencies: `mz-lowertest-derive`, `mz-ore`, `proc-macro2`, `serde_json`, `itertools`.
Consumed by test harnesses throughout the Materialize codebase (e.g., `mz-expr`, `mz-repr`, `mz-transform`) that use datadriven test files.
