---
source: src/testdrive/src/action/sql_server/connect.rs
revision: 67a183f8d5
---

# testdrive::action::sql_server::connect

Implements the `sql-server-connect` builtin command, which establishes a named SQL Server connection from an ADO connection string supplied in the command's input block.
Stores the resulting `mz_sql_server_util::Client` in the testdrive state by name.
