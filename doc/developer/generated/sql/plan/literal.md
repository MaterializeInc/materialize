---
source: src/sql/src/plan/literal.rs
revision: a458244f1b
---

# mz-sql::plan::literal

Converts AST `IntervalValue` to `mz_repr::Interval` (via `plan_interval`) and back (via `unplan_interval`), bridging the parser's datetime field enum to the ADT representation.
