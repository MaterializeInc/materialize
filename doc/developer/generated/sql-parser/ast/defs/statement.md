---
source: src/sql-parser/src/ast/defs/statement.rs
revision: 1e050fdd07
---

# mz-sql-parser::ast::defs::statement

Defines `Statement<T>`, the top-level enum of all SQL statement types supported by Materialize, including DML (SELECT, INSERT, UPDATE, DELETE, COPY), DDL (CREATE/ALTER/DROP for connections, databases, schemas, sources, sinks, tables, views, materialized views, indexes, secrets, clusters, roles, types, functions, continual tasks), and control statements (SUBSCRIBE, EXPLAIN, SHOW, SET, RESET, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, DECLARE, FETCH, CLOSE, INSPECT, RAISE).
Each statement variant has a corresponding struct with its specific fields.
