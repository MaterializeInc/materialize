---
source: src/sql/src/plan/with_options.rs
revision: 4267863081
---

# mz-sql::plan::with_options

Defines the `TryFromValue` and `ImpliedValue` traits used by `generate_extracted_config!`-generated types, plus `TryFromValue` implementations for a large set of Rust types (`String`, `bool`, numerics, `Duration`, `Interval`, `ByteSize`, connection references, etc.).
This is the type conversion layer that bridges raw AST `WithOptionValue` tokens to typed Rust values consumed by planner and purification logic.
