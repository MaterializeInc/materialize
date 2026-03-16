---
source: src/expr/src/scalar/func/unmaterializable.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::unmaterializable

Defines the `UnmaterializableFunc` enum, representing session- and environment-dependent SQL functions that cannot be folded at planning time.
Variants include `CurrentDatabase`, `CurrentTimestamp`, `MzNow`, `MzVersion`, `SessionUser`, `ViewableVariables`, and similar functions; evaluation is deferred to `mz-adapter`.
Each variant implements `output_type` to report its return type for type inference purposes.
