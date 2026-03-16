---
source: src/sql/src/pure/sql_server.rs
revision: 10ba6116a5
---

# mz-sql::pure::sql_server

SQL Server-specific purification helpers: `purify_source_exports` resolves `ExternalReferences` against `RetrievedSourceReferences` retrieved from SQL Server and builds `CreateSubsourceStatement` ASTs including text/exclude-column normalization.
`PurifiedSourceExports` carries the resulting subsource map and normalized column lists.
