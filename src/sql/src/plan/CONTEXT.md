# sql::plan

The SQL compiler's output layer: converts a purified, name-resolved
`Statement<Aug>` into a `Plan` — the typed, side-effect-free description of
what the adapter must do.

## Files (LOC ≈ 36,204 total)

| File / Dir | LOC | What it owns |
|---|---|---|
| `statement/` | 13,917 | Statement dispatch + all DDL/DML/ACL/SCL/TCL planners (see `statement/CONTEXT.md`) |
| `query.rs` | 6,826 | SQL query → `HirRelationExpr`: `plan_root_query`, `plan_expr`, joins, subqueries, GROUP BY, window functions; `ExprContext`/`QueryContext` |
| `hir.rs` | 4,055 | HIR IR: `HirRelationExpr`, `HirScalarExpr`, `ColumnRef` (leveled), `CoercibleScalarExpr`; `lower()` entry point to decorrelation |
| `lowering.rs` | 2,650 | HIR → MIR decorrelation; `ColumnMap`/outer-relation lifting; delegates variadic-left optimization to `lowering/variadic_left.rs` |
| `statement.rs` (root) | 1,151 | `describe()`, `plan()` dispatch; `StatementContext`, `StatementDesc` |
| `plan.rs` (module root) | 2,202 | `Plan` enum (82 variants), all plan-specific structs (`CreateSourcePlan`, `SelectPlan`, etc.), `PlanContext`, `QueryLifetime` |
| `error.rs` | 1,079 | `PlanError` enum (~116 variants), `From` impls |
| `with_options.rs` | 970 | `generate_extracted_config!` macro and `TryFromValue` helpers |
| `transform_ast.rs` | 872 | Pre-planning AST rewrites (e.g. `NATURAL JOIN` expansion) |
| `transform_hir.rs` | 782 | HIR rewrites applied before lowering |
| `typeconv.rs` | 1,573 | Cast catalog and coercion rules |
| `explain/` | 507 | `ExplainPlan` text rendering (`explain/text.rs`) |
| `scope.rs` | 570 | `Scope` — column visibility tracking during query planning |
| `side_effecting_func.rs` | 296 | Planning for side-effecting functions (e.g. `pg_cancel_backend`) |
| `notice.rs`, `literal.rs`, `plan_utils.rs`, `virtual_syntax.rs` | < 120 each | Planning notices, literal coercion, utilities, LATERAL/values virtual syntax |

## Key concepts

- **`Plan` enum** — 82 variants, one per SQL statement category, produced by
  `plan::statement::plan()` and consumed by `mz_adapter::coord::sequencer`.
  Each variant carries a typed plan struct (e.g. `CreateSourcePlan`,
  `SelectPlan`).
- **HIR** — the compiler's mid-level IR. Admits correlated subqueries and
  outer-column references (`ColumnRef { level > 0 }`). Lives entirely in
  `plan/`; never escapes to `mz_adapter` or `mz_expr`.
- **Decorrelation** — `HirRelationExpr::lower()` → `lowering.rs` → `MirRelationExpr`.
  The `variadic_left` specialization handles stacks of LEFT JOINs.
- **Purification boundary** — `plan::statement::plan()` is pure and synchronous.
  All external-state inlining (schema registries, upstream catalogs) happens
  before this call, in `crate::pure`.
- **`StatementContext`** — planning state: `&dyn SessionCatalog`, optional
  `PlanContext` (absent for view bodies), parameter-type accumulator.

## Subtree highlights

- `statement/ARCH_REVIEW.md` — `ddl.rs` (8,137 LOC, 110 functions) has grown
  to 30 DDL object domains; splitting sources + sinks + clusters into
  `ddl/source.rs`, `ddl/sink.rs`, `ddl/cluster.rs` follows the established
  `ddl/connection.rs` precedent with low risk.

## Cross-references

- Upstream: `crate::pure` (purification), `crate::names` (name resolution),
  `crate::catalog::SessionCatalog`.
- Downstream: `mz_adapter::coord::sequencer` dispatches on `Plan` variants.
- Generated docs: `doc/developer/generated/sql/plan/`.
