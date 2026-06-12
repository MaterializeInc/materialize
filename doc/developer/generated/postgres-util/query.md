---
source: src/postgres-util/src/query.rs
revision: 12fbe31d24
---

# mz-postgres-util::query

Provides typed SQL query helpers built around the `Sql` type from `mz_ore::sql`. The module re-exports `Sql`, `SqlFormatError`, and `SqlTemplateError` from `mz_ore::sql`.

The following functions are provided:
* `simple_query_opt` — runs a query via `Client::simple_query` and expects at most one row; returns `Ok(Some(row))`, `Ok(None)`, or `Err(PostgresError::UnexpectedRow)`.
* `simple_query` — runs a simple query and returns all protocol messages.
* `query` / `query_prepared` — run a parameterized query and return all resulting rows.
* `query_one` / `query_one_prepared` — run a query and return exactly one row.
* `query_opt` / `query_opt_prepared` — run a query and return at most one row.
* `execute` / `execute_prepared` — run a query and return the number of affected rows.
* `batch_execute` — run one or more SQL statements with no returned rows.

All functions accept `Sql` (for ad-hoc queries) or `&Statement` (for the `_prepared` variants) to enforce safe SQL construction.
