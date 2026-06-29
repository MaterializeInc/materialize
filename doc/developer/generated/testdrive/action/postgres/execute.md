---
source: src/testdrive/src/action/postgres/execute.rs
revision: 12fbe31d24
---

# testdrive::action::postgres::execute

Implements the `postgres-execute` builtin command, which runs each line of the command's input block as a separate SQL statement against a named or ad-hoc PostgreSQL connection.
Supports both pre-established named connections (via `postgres-connect`) and inline URL connections.
