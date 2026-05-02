# mz-expr

The Mid-level Intermediate Representation (MIR) crate for Materialize.
Defines all IR types shared between the SQL planner, optimizer, and compute
execution layers. Pure data-structure library: no I/O, no catalog access.

## Top-level files (12,951 LOC, excluding subdirs)

| File | What it owns |
|---|---|
| `relation.rs` (4,470) | `MirRelationExpr` enum (15 variants), `MapFilterProject`, `RowSetFinishing`, `RowComparator`, `CollectionPlan`, `JoinImplementation` |
| `scalar.rs` (3,426) | `MirScalarExpr` enum, `EvalError`, `eval`, simplification/folding |
| `linear.rs` (1,988) | `MapFilterProject` lowering to `MfpPlan`/`SafeMfpPlan`; column-permutation helpers |
| `interpret.rs` (1,778) | Abstract interpreter (`ResultSpec`, `ColumnSpecs`, `Interpreter`, `Trace`) for filter-pushdown |
| `visit.rs` (1,189) | `VisitChildren` / `Visit` traits; stack-safe traversal with `RecursionGuard` |
| `id.rs` (≈80) | `Id` (Local/Global), `LocalId`, `SourceInstanceId` |
| `lib.rs` (100) | Crate root; re-exports; feature flags |
| `explain.rs` / `virtual_syntax.rs` / `row.rs` | EXPLAIN text/JSON rendering; virtual-syntax helpers; `RowCollection` |

## Subdirectories

| Dir | LOC | What it owns |
|---|---|---|
| `scalar/` (16,078) | `MirScalarExpr` + full scalar function library *(see [`src/scalar/CONTEXT.md`](src/scalar/CONTEXT.md))* |
| `relation/` (5,061) | `AggregateFunc`, `TableFunc`, window functions, join utilities *(see [`src/relation/CONTEXT.md`](src/relation/CONTEXT.md))* |

## Key concepts

- **MIR** — the IR layer between SQL planning (`mz-sql`) and optimization (`mz-transform`) / execution (`mz-compute`). Purely data-structure: no I/O, no catalog access.
- **`VisitChildren` / `Visit`** — generic traversal; both `MirScalarExpr` and `MirRelationExpr` implement `VisitChildren`; `Visit` is auto-derived, giving pre/post/mutable/fallible traversal for free.
- **`MapFilterProject` (MFP)** — fuses `Map → Filter → Project` into one operator; critical for pushing computation close to sources. Lowered to `MfpPlan` / `SafeMfpPlan` for execution.
- **Abstract interpreter** (`interpret.rs`) — propagates `ResultSpec` (range + nullability overapproximation) through expressions; drives filter-pushdown decisions in the optimizer and EXPLAIN output.
- **`RECURSION_LIMIT = 2048`** — large limit to handle pathological `Let`-chain and associative-operator-chain patterns.
- **`UnmaterializableFunc`** — session-context functions deferred to `mz-adapter` for evaluation; `MirScalarExpr::eval` cannot handle them.

## Primary exports
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
