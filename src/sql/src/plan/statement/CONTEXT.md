# sql::plan::statement

Statement-level planning dispatch: converts a purified `Statement<Aug>` into a
`Plan` variant via two entry points — `describe()` (returns `StatementDesc`,
parameter types, copy flag) and `plan()` (returns the concrete `Plan`).
`plan_copy_from` is a third entry point for the COPY ingestion path.

## Files (LOC ≈ 13,917 total)

| File | What it owns |
|---|---|
| `statement.rs` | Dispatch switch for `describe` + `plan`; `StatementContext` / `StatementDesc` types |
| `ddl.rs` | 8,137 LOC — `describe_*` / `plan_*` for all DDL: databases, schemas, tables, sources, sinks, views, MVs, indexes, connections, secrets, clusters, roles, network policies, continual tasks, types, comments, all ALTER variants |
| `ddl/connection.rs` | Connection-specific plan helpers, split from `ddl.rs` |
| `dml.rs` | SELECT, INSERT, UPDATE, DELETE, SUBSCRIBE, COPY, all EXPLAIN variants |
| `show.rs` | SHOW * statements — most rewritten to internal SELECT against `mz_catalog` |
| `acl.rs` | GRANT/REVOKE/ALTER OWNER/ALTER DEFAULT PRIVILEGES/REASSIGN OWNED |
| `scl.rs` | Session control: DECLARE, PREPARE, EXECUTE, CLOSE, FETCH, SET/RESET VARIABLE, INSPECT SHARD |
| `tcl.rs` | Transaction control: BEGIN/COMMIT/ROLLBACK/SET TRANSACTION |
| `raise.rs` | RAISE statement |
| `validate.rs` | VALIDATE CONNECTION |

## Key concepts

- **`StatementContext`** — immutable planning state for a single statement: catalog ref, `PlanContext` (optional; absent for view definitions), parameter type accumulator (`RefCell<BTreeMap>`), and `ambiguous_columns` flag.
- **`StatementDesc`** — output of `describe`: `Option<RelationDesc>`, inferred `param_types`, `is_copy` flag.
- **`ddl.rs` as monolith** — 110 public `describe_*`/`plan_*` function pairs covering ~30 DDL object types. Source-connector-specific helpers (`plan_kafka_source_connection`, `plan_postgres_source_connection`, etc.) live inline. `ddl/connection.rs` extracted one sub-domain (connection types); all other object domains remain in the single file.
- **Parallel `describe/plan` pattern** — every statement has both a `describe_*` and a `plan_*` function. `describe_*` is usually a 3-line stub returning `StatementDesc::new(None)`; the actual logic is in `plan_*`.

## Cross-references

- Input: `mz_sql_parser::ast::Statement<Aug>` (name-resolved AST).
- Output: `crate::plan::Plan` consumed by `mz_adapter`.
- `StatementContext` wraps `SessionCatalog` (`src/sql/src/catalog.rs`).
- `ddl.rs` imports from `pure.rs` types (connector options extracted during purification).
- Generated docs: `doc/developer/generated/sql/plan/statement/`.
