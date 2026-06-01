---
source: src/expr/src/lib.rs
revision: 5d046b3ab6
---

# mz-expr

The core expression language for Materialize, defining the Mid-level Intermediate Representation (MIR) used throughout the optimizer and dataflow layers.

The crate provides two primary IR types: `MirScalarExpr` (scalar expressions including column references, literals, and function calls) and `MirRelationExpr` (relational algebra operators such as `Filter`, `Join`, `Reduce`, `TopK`, etc.).
Supporting modules cover the full scalar function library (`scalar::func`), the fused `MapFilterProject` operator (`linear`), an abstract interpreter for filter pushdown (`interpret`), visitor infrastructure (`visit`), `EXPLAIN` rendering (`explain`), and sorted row collections (`row`).

Key internal dependencies: `mz-repr` (datum/row types), `mz-expr-derive` (the `#[sqlfunc]` proc-macro), `mz-proto` (protobuf serialization), `mz-pgrepr`/`mz-pgtz` (PostgreSQL type compatibility), and `mz-ore` (utilities).
The crate also exports the `Columns` and `Eval` traits, which abstract over column-reference operations and scalar evaluation respectively, enabling code to work with both MIR-level and future LIR-level scalar expressions.
Downstream consumers include `mz-sql` (which lowers SQL AST to MIR), `mz-transform` (which optimizes MIR), `mz-compute` (which executes MIR), and `mz-adapter` (which evaluates unmaterializable functions).
