---
source: src/testdrive/src/action/duckdb.rs
revision: fa066df3e5
---

# testdrive::action::duckdb

Groups the two DuckDB builtin commands and provides the shared `get_or_create_connection` helper, which lazily opens a DuckDB connection by name and stores it in the testdrive state.
DuckDB connections are wrapped in `Arc<Mutex<Connection>>` to allow safe sharing across blocking tasks.
