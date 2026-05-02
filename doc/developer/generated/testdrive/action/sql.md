---
source: src/testdrive/src/action/sql.rs
revision: 35fe074497
---

# testdrive::action::sql

Executes SQL commands and expected-failure SQL commands against Materialize via the PostgreSQL wire protocol.
`run_sql` sends a query and compares the sorted result rows (or an MD5 hash) against the expected output, retrying with exponential backoff for queries that may return stale data; DDL and FETCH statements are not retried.
`run_fail_sql` executes a statement that is expected to fail and verifies the error message using one of four match modes (contains, exact, regex, or timeout).
`decode_row` converts `tokio_postgres::Row` values to strings for all supported Materialize types, applying the session's regex substitution if set.
