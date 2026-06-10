---
source: src/postgres-util/src/query.rs
revision: da455a1559
---

# mz-postgres-util::query

Provides `simple_query_opt`, a helper that runs a query via `Client::simple_query` and expects at most one row.
Returns `Ok(Some(row))` for a single row, `Ok(None)` for no rows, and `Err(PostgresError::UnexpectedRow)` if more than one row is returned.
