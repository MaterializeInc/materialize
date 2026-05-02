---
source: src/testdrive/src/action/sql_server/execute.rs
revision: 9419e9ad0d
---

# testdrive::action::sql_server::execute

Implements the `sql-server-execute` builtin command, which runs each line of the input block (or the entire block as one statement when `split-lines=false`) against a named SQL Server connection.
Automatically retries statements that fail due to SQL Server deadlock (error 1205), SQL Server Agent startup delays (error 14258), or database-not-yet-available errors (error 904) up to 20 times with a 100ms backoff.
Supports an `abandon-txn` mode that wraps the SQL in a transaction and drops it without committing, used to test that `Transaction::drop` correctly sends a ROLLBACK.
