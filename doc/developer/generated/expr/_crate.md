---
source: src/expr/src/lib.rs
revision: 40e5dd1af8
---

# mz-expr

The core expression language for Materialize, defining the Mid-level Intermediate Representation (MIR) used throughout the optimizer and dataflow layers.

The crate provides two primary IR types: `MirScalarExpr` (scalar expressions including column references, literals, and function calls) and `MirRelationExpr` (relational algebra operators such as `Filter`, `Join`, `Reduce`, `TopK`, etc.).
Supporting modules cover the full scalar function library (`scalar::func`), the fused `MapFilterProject` operator (`linear`), an abstract interpreter for filter pushdown (`interpret`), visitor infrastructure (`visit`), `EXPLAIN` rendering (`explain`), and sorted row collections (`row`).

Key internal dependencies: `mz-repr` (datum/row types), `mz-expr-derive` (the `#[sqlfunc]` proc-macro), `mz-proto` (protobuf serialization), `mz-pgrepr`/`mz-pgtz` (PostgreSQL type compatibility), and `mz-ore` (utilities).
The crate also exports the `Columns`, `Eval`, and `OptimizableExpr` traits. `Columns` abstracts over column-reference operations; `Eval` abstracts over scalar evaluation; `OptimizableExpr` is the bound required by the generic `MapFilterProject<E>` parameter, combining the common operations needed for optimization (temporal predicate detection, error-literal detection, and column manipulation) into a single trait.
Downstream consumers include `mz-sql` (which lowers SQL AST to MIR), `mz-transform` (which optimizes MIR), `mz-compute` (which executes MIR), and `mz-adapter` (which evaluates unmaterializable functions).
