---
source: src/sql/src/plan/lowering.rs
revision: 703a0c27c8
---

# mz-sql::plan::lowering

Translates HIR to MIR via decorrelation.
The root file drives the traversal and maintains the `ColumnMap`/outer-relation state; `variadic_left` contributes an optimization for stacks of uncorrelated left joins.
