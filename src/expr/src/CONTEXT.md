# expr::src (mz-expr crate root)

The Mid-level Intermediate Representation (MIR) for Materialize. Defines the
two primary IR types (`MirScalarExpr`, `MirRelationExpr`), the complete scalar
function library, traversal infrastructure, a fused map-filter-project operator,
an abstract interpreter, and EXPLAIN rendering.

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
| `scalar/` (16,078) | `MirScalarExpr` + full scalar function library *(see `scalar/CONTEXT.md`)* |
| `relation/` (5,061) | `AggregateFunc`, `TableFunc`, window functions, join utilities *(see `relation/CONTEXT.md`)* |

## Key concepts

- **MIR** — the IR layer between SQL planning (`mz-sql`) and optimization
  (`mz-transform`) / execution (`mz-compute`). Purely data-structure: no I/O,
  no catalog access.
- **`VisitChildren` / `Visit`** — generic traversal; both `MirScalarExpr` and
  `MirRelationExpr` implement `VisitChildren`; `Visit` is auto-derived, giving
  pre/post/mutable/fallible traversal for free.
- **`MapFilterProject` (MFP)** — fuses `Map → Filter → Project` into one
  operator; critical for pushing computation close to sources. Lowered to
  `MfpPlan` / `SafeMfpPlan` for execution.
- **Abstract interpreter** (`interpret.rs`) — propagates `ResultSpec`
  (range + nullability overapproximation) through expressions; drives
  filter-pushdown decisions in the optimizer and EXPLAIN output.
- **`RECURSION_LIMIT = 2048`** — large limit to handle pathological
  `Let`-chain and associative-operator-chain patterns.
- **`UnmaterializableFunc`** — session-context functions deferred to
  `mz-adapter` for evaluation; `MirScalarExpr::eval` cannot handle them.

## Bubbled in from subdirs

- `scalar/func/ARCH_REVIEW.md` — two items:
  1. `derive_unary!`/`derive_binary!`/`derive_variadic!` macros carry stale
     "temporary, will use `enum_dispatch`" comments; `enum_dispatch` is not
     in `Cargo.toml`.
  2. Container-cast `introduces_nulls` is conservatively `true` at 6+ sites
     due to a crate-boundary gap with `mz-sql::typeconv`.

## What should bubble up to src/CONTEXT.md

- **MIR crate identity**: `mz-expr` is the shared IR hub; `mz-sql` writes it,
  `mz-transform` optimizes it, `mz-compute` executes it, `mz-adapter`
  evaluates unmaterializable functions. No other crate in the stack owns the
  relational or scalar expression types.
- **`mz-expr-derive`** (`#[sqlfunc]` proc-macro) is a required companion crate;
  understanding `mz-expr` without it is incomplete.
- **Seam note**: `mz-expr` intentionally has no dependency on `mz-sql`. The
  typeconv-nullability gap (see `scalar/func/ARCH_REVIEW.md`) is a direct
  consequence of this boundary.

## Cross-references

- Downstream: `mz-sql` (lowering), `mz-transform` (optimization), `mz-compute`
  (execution), `mz-adapter` (unmaterializable eval).
- `mz-repr` provides `Datum`, `Row`, `RowArena`, `ReprColumnType`.
- Generated docs: `doc/developer/generated/expr/`.
