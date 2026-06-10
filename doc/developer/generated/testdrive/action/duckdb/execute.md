---
source: src/testdrive/src/action/duckdb/execute.rs
revision: fa066df3e5
---

# testdrive::action::duckdb::execute

Implements the `duckdb-execute` builtin command, which runs the input block as SQL against a named DuckDB connection.
Spawns a blocking task to avoid blocking the async runtime during query execution.
