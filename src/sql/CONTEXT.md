# sql (mz-sql crate)

SQL-to-dataflow translation: the two-phase compiler that converts SQL text into
`Plan` values consumed by the adapter. Phase 1 is async purification; Phase 2 is
pure planning.

## Module structure (LOC ≈ 65,741 total)

| Module | LOC | Role |
|---|---|---|
| `plan/` | 36,204 | SQL compiler: `Plan` enum, HIR IR, decorrelation, statement planners |
| `session/` | 6,919 | Session/system variables, `FeatureFlag`, user definitions |
| `func.rs` | 6,111 | Built-in function + operator resolution, overload matching |
| `pure.rs` + `pure/` | 5,928 | Async purification for CREATE/ALTER SOURCE, sinks, MV options |
| `names.rs` | 2,628 | All name types: `Aug`, `FullItemName`, `ResolvedItemName`, `ObjectId`, etc. |
| `catalog.rs` | 1,991 | `SessionCatalog` trait (67 methods) + catalog item/type traits |
| `rbac.rs` | 1,883 | Role-based access control checks (`check_plan`, `check_usage`) |
| `normalize.rs` | 730 | AST normalization, `generate_extracted_config!` macro |
| `ast.rs` | thin re-export | Re-exports `mz_sql_parser::ast` + rename/rewrite helpers |
| `parse.rs` | thin re-export | Re-exports `mz_sql_parser::parser` |
| `kafka_util.rs`, `iceberg.rs` | small | Connector-specific WITH-option helpers |
| `optimizer_metrics.rs` | small | Prometheus metrics for optimization latency |

## Key interfaces and seams

- **`SessionCatalog` trait** (`catalog.rs`) — 67-method trait; the single seam
  between the SQL planner and the catalog. Only one concrete impl:
  `ConnCatalog` in `mz_adapter`. Wraps resolution, lookup, privilege
  queries, session-variable access, and notice emission.
- **`pure::purify_statement`** — async entry point for Phase 1. Dispatches on
  statement type; source-connector logic delegates to `pure/{postgres,mysql,
  sql_server}.rs` and `pure/references.rs`.
- **`plan::statement::{describe, plan}`** — sync entry points for Phase 2.
  Return `StatementDesc` and `Plan` respectively.
- **`Plan` enum** — 82 variants; consumed exclusively by
  `mz_adapter::coord::sequencer`.
- **`Aug`** — zero-size marker type; `Statement<Aug>` carries fully-resolved
  names (post name-resolution, pre-planning).

## Architectural notes

- The two-phase split (purify then plan) is explicitly documented in `lib.rs`:
  purification inlines external state so planning can be a pure, fast,
  catalog-locking function.
- `SessionCatalog` has exactly one impl in the entire codebase
  (`ConnCatalog<'_>` in `mz_adapter`). The trait boundary is load-bearing for
  testing (mock catalogs in tests), not for runtime polymorphism.
- `func.rs` (6,111 LOC) is a large but coherent function-resolution table;
  deletion test: removing it forces function calls to go unresolved. Complexity
  would vanish if a code-gen approach were used, but that is a scope-level
  question, not a seam friction.

## Subtree findings to bubble up

- **`plan/statement/ARCH_REVIEW.md`**: `ddl.rs` is 8,137 LOC / 110 functions
  across ~30 object domains. Splitting `source`, `sink`, and `cluster` subsets
  into `ddl/source.rs`, `ddl/sink.rs`, `ddl/cluster.rs` follows the existing
  `ddl/connection.rs` precedent and removes cross-domain navigation cost with
  no behavioral risk.
- **`session/vars.rs`**: `SESSION_SYSTEM_VARS` is a hardcoded parallel list of
  19 system vars that are also settable per-session. An existing TODO
  (parkmycar) notes this should become a field on `VarDefinition`. Low impact
  but a latent source of divergence when new session-settable system vars are
  added.

## Subdirs reviewed (≥5K LOC)

- [`src/plan/`](src/plan/CONTEXT.md) — SQL compiler: `Plan` enum, HIR IR, decorrelation, statement planners (36,204 LOC)
- [`src/session/`](src/session/CONTEXT.md) — Session and system variables, `FeatureFlag`, user definitions (6,919 LOC)

Other subdirs (`ast.rs`, `pure/`, `func.rs`, `catalog.rs`, `rbac.rs`, etc.) are summarized in the module structure table above.

## Cross-references

- Primary downstream consumer: `mz_adapter` (catalog impl, sequencer,
  purification call sites).
- Key upstream dependencies: `mz_sql_parser`, `mz_expr`, `mz_repr`,
  `mz_storage_types`, `mz_adapter_types`.
- Generated docs: `doc/developer/generated/sql/`.
