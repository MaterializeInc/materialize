---
source: src/testdrive/src/action/mysql/connect.rs
revision: fc407b9154
---

# testdrive::action::mysql::connect

Implements the `mysql-connect` builtin command, which establishes a named MySQL connection using `mysql_async`.
The optional `password` argument allows the password to be supplied separately from the URL, accommodating passwords with special characters.
