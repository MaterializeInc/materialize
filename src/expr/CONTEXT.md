# mz-expr

The Mid-level Intermediate Representation (MIR) crate for Materialize.
Defines all IR types shared between the SQL planner, optimizer, and compute
execution layers. Pure data-structure library: no I/O, no catalog access.

## Crate contents

All source lives under `src/` (36,891 LOC). See `src/CONTEXT.md` for the
module-level breakdown.

Primary exports:
- `MirScalarExpr` — scalar expression tree (column refs, literals, function calls, `If`)
- `MirRelationExpr` — relational operator tree (Filter, Join, Reduce, TopK, …)
- `UnaryFunc` / `BinaryFunc` / `VariadicFunc` / `UnmaterializableFunc` — 580+
  scalar function variants
- `AggregateFunc` / `TableFunc` — aggregate and set-returning functions
- `MapFilterProject` (`MfpPlan`, `SafeMfpPlan`) — fused map-filter-project
- `Visit` / `VisitChildren` — stack-safe generic traversal
- `EvalError` — comprehensive runtime error enum

## Position in the dependency graph

```
mz-sql  ──lowering──►  mz-expr  ◄──optimize──  mz-transform
                          │
                     ◄────┼────►  mz-compute (execute)
                          │
                     ◄────┘  mz-adapter (unmaterializable eval)
```

Key internal deps: `mz-repr` (Datum/Row), `mz-expr-derive` (`#[sqlfunc]`
proc-macro), `mz-proto` (protobuf), `mz-pgrepr`/`mz-pgtz` (PG compat).

## Architecture findings

See `src/scalar/func/ARCH_REVIEW.md` for two surfaced items:
1. `derive_unary!`/`derive_binary!`/`derive_variadic!` macros carry stale
   "will eventually use `enum_dispatch`" comments; the crate does not depend on
   `enum_dispatch` and the macros work correctly as-is.
2. Container-cast functions (`CastStringToList`, `CastList1ToList2`, etc.) at 6+
   sites return `introduces_nulls = true` conservatively because accurate
   nullability depends on `mz-sql::typeconv`, which is outside this crate.

## What should bubble to src/CONTEXT.md

- `mz-expr` is the single IR hub of the stack; every layer above `mz-repr`
  reads or writes it.
- The crate-boundary between `mz-expr` and `mz-sql` (no `mz-sql` dep in
  `mz-expr`) is an intentional design constraint; it is the root cause of the
  typeconv-nullability gap.
- `mz-expr-derive` is a required companion; the `#[sqlfunc]` proc-macro is
  central to how the 580+ function variants are defined.

## Cross-references

- Generated docs: `doc/developer/generated/expr/_crate.md` and
  `doc/developer/generated/expr/`.
