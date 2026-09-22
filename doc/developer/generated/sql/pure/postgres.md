---
source: src/sql/src/pure/postgres.rs
revision: 8b1c640604
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types to Materialize types by producing `StorageScalarExpr`-based cast expressions (including OID-based casts), and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
Privilege and replica-identity queries are issued via `mz_postgres_util::query` with SQL literals constructed using the `sql!` macro, rather than calling `client.query` directly.
When generating subsource statements, table constraints (unique keys) are omitted for any key whose columns are not all present in the subsource — for example when some columns are excluded via `EXCLUDE COLUMNS`. Only keys where every referenced column is included are propagated.
`generate_column_casts` takes a `cast_oid_full_range: bool` parameter that is passed through to `pg_type_to_cast_func`; newly purified exports always set `cast_oid_full_range: true` in their `SourceExportStatementDetails::Postgres`.
The subsource-generation helper accepts `exclude_constraints: &BTreeSet<String>` and `exclude_all_constraints: bool` parameters. When `exclude_constraints` is non-empty, it validates each named constraint against the table's `PRIMARY KEY` and `UNIQUE` constraints (case-sensitively) and removes matching ones, returning `PgSourcePurificationError::ConstraintsNotFound` for any name that does not match. When `exclude_all_constraints` is true, all keys are cleared and every column is marked nullable, allowing the upstream table to drop or add constraints without causing a Materialize outage.
