---
source: src/ore/src/overflowing.rs
revision: 4267863081
---

# mz-ore::overflowing

Defines `Overflowing<T>`, a numeric wrapper that performs checked arithmetic on all standard integer operations (`+`, `-`, `*`, `/`, `%`, negation) and invokes a configurable overflow handler instead of silently wrapping.
The overflow behavior — `Panic`, `SoftPanic`, or `Ignore` — is controlled at runtime via `set_behavior` and stored in a global atomic; the default is `Ignore` in release builds.
The type is implemented for all primitive integer types via macros, and optionally integrates with `differential-dataflow` difference types, `columnation`, `columnar`, and `num-traits`.
