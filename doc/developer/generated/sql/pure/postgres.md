---
source: src/sql/src/pure/postgres.rs
revision: 0c7c2a15c1
---

# mz-sql::pure::postgres

Postgres-specific purification helpers: validates SELECT/RLS/replica-identity privileges on requested tables, maps Postgres column types to Materialize types by producing `StorageScalarExpr`-based cast expressions (including OID-based casts), and generates `CreateSubsourceStatement` ASTs from `PostgresTableDesc` descriptions.
Consumes a live `tokio_postgres::Client` to introspect the upstream publication.
Privilege and replica-identity queries are issued via `mz_postgres_util::query` with SQL literals constructed using the `sql!` macro, rather than calling `client.query` directly.
When generating subsource statements, table constraints (unique keys) are omitted for any key whose columns are not all present in the subsource — for example when some columns are excluded via `EXCLUDE COLUMNS`. Only keys where every referenced column is included are propagated.
`generate_column_casts` takes a `cast_oid_full_range: bool` parameter that is passed through to `pg_type_to_cast_func`; newly purified exports always set `cast_oid_full_range: true` in their `SourceExportStatementDetails::Postgres`.
`purify_source_exports` accepts a `filter_constraints: &FilterConstraints` parameter (replacing the earlier `exclude_constraints: &BTreeSet<String>` and `exclude_all_constraints: bool` pair). `initial_lsn` is an upper bound on the upstream WAL position whose schemas the retrieved references describe; it is read after the references are fetched so it accounts for any schema change they already reflect. Each purified export carries this `initial_lsn` in its `PurifiedExportDetails::Postgres`, which planning then stores in `SourceExportStatementDetails::Postgres` (as `Some(initial_lsn)`) and ultimately in `PostgresSourceExportDetails`. When `FilterConstraints::Exclude(names)` is set, it validates each named constraint against the table's `PRIMARY KEY` and `UNIQUE` constraints (case-sensitively) and removes matching ones, returning `PgSourcePurificationError::ConstraintsNotFound` for any name that does not match. When `FilterConstraints::ExcludeAll` is set, all keys are cleared and every column is marked nullable, allowing the upstream table to drop or add constraints without causing a Materialize outage. Constraint filtering is only valid when exactly one export is being purified.
