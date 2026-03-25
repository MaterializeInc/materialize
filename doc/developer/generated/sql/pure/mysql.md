---
source: src/sql/src/pure/mysql.rs
revision: 844ad57e4b
---

# mz-sql::pure::mysql

MySQL-specific purification helpers: converts `ExternalReferences` and `RetrievedSourceReferences` into `CreateSubsourceStatement` ASTs, validates table references (two-part names only), and maps MySQL column types to Materialize types.
Defines `MYSQL_DATABASE_FAKE_NAME` (the synthetic database name used to fit MySQL's two-layer hierarchy into the three-layer catalog model).
