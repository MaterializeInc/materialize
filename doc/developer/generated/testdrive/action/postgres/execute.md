---
source: src/testdrive/src/action/postgres/execute.rs
revision: 3b6acccfd
---

# testdrive::action::postgres::execute

Implements the `postgres-execute` builtin command, which runs each line of the command's input block as a separate SQL statement against a named or ad-hoc PostgreSQL connection.
Supports both pre-established named connections (via `postgres-connect`) and inline URL connections. Two connection names are built in and require no prior `postgres-connect`: `mz_system` (connects as `mz_system` to the internal SQL address) and `materialize` (connects as `materialize` to the public SQL address). An explicit `postgres-connect` registration with the same name takes precedence over the built-in URL.
