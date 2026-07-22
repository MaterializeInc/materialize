---
source: src/testdrive/src/action/sql_server/connect.rs
revision: 47f491f3f4
---

# testdrive::action::sql_server::connect

Implements the `sql-server-connect` builtin command, which establishes a named SQL Server connection from an ADO connection string supplied in the command's input block.
Stores the resulting `mz_sql_server_util::Client` in the testdrive state by name.
The command accepts optional retry arguments (`retry-initial-backoff`, `retry-clamp-backoff`, `retry-max-duration`, all in seconds) to handle transient Azure SQL Database errors such as error 40613 ("Database is not currently available"), which occurs while a serverless database resumes or scales. Defaults are generous (initial 500 ms, clamp 5 s, max 90 s) to accommodate databases that can take tens of seconds to come online.
