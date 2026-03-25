---
source: src/materialized/src/bin/materialized.rs
revision: 7defc6a810
---

# mz-materialized

Provides the single unified binary that dispatches to either `mz-clusterd` or `mz-environmentd` depending on the name under which it is invoked (`clusterd` or `environmentd`).
The crate contains no logic of its own; it re-exports the `main` functions of its two dependencies and uses the executable filename to select the right entry point.
Key dependencies are `mz-clusterd` and `mz-environmentd`.
