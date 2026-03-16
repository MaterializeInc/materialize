---
source: src/adapter/src/coord/sql.rs
revision: 7340287c14
---

# adapter::coord::sql

Implements the coordinator's SQL-layer entry points: `parse`, `plan`, and the `prepare`/`execute` steps of the extended query protocol.
`plan` resolves names against the catalog, invokes the SQL planner, and returns a `Plan`; `prepare` additionally describes the statement's parameter and result types for the pgwire Describe flow.
