---
source: src/storage/src/source/sql_server/replication.rs
revision: b0fa98e931
---

# mz-storage::source::sql_server::replication

Renders the replication operator for SQL Server CDC ingestion, handling per-capture-instance snapshots and streaming CDC events in LSN order from a single designated worker.
Performs initial table snapshots within a CDC transaction to capture consistent LSN boundaries, manages rewind logic to reconcile snapshot data with subsequent change events, tracks deferred updates for Large Object Data (LOD) types across batch boundaries, validates restore history to detect database restores, handles schema change events, and decodes rows into `SourceMessage` records distributed by partition index.
