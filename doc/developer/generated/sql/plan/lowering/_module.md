---
source: src/sql/src/plan/lowering.rs
revision: 6168aa99a7
---

# mz-sql::plan::lowering

Translates HIR to MIR via decorrelation.
The root file drives the traversal and maintains the `ColumnMap`/outer-relation state; `variadic_left` contributes an optimization for stacks of uncorrelated left joins.
