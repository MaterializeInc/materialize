---
source: src/testdrive/src/action/sql_server/execute.rs
revision: ce0bdd82fd
---

# testdrive::action::sql_server::execute

Implements the `sql-server-execute` builtin command, which runs each line of the input block against a named SQL Server connection.
Automatically retries statements that fail due to SQL Server deadlock (error 1205) up to a configured number of times.
