---
source: src/sql-server-util/src/cdc.rs
revision: 4267863081
---

# mz-sql-server-util::cdc

Implements SQL Server Change Data Capture replication via `CdcStream`, which wraps a `Client` and provides `snapshot` and `into_stream` methods.
`snapshot` acquires a consistent snapshot and LSN by: locking the table under READ COMMITTED on a fencing connection, starting a SNAPSHOT isolation transaction on a second connection, creating a savepoint to generate an LSN, releasing the lock, then streaming the table rows.
`into_stream` polls CDC change tables on a configurable interval, emitting `CdcEvent::Data` (grouped by LSN), `CdcEvent::Progress` (next LSN watermark), and `CdcEvent::SchemaUpdate` (DDL changes) events.
`Lsn` is a three-part (vlf_id, block_id, record_id) timestamp that implements `timely::progress::Timestamp` for use in dataflow progress tracking; `Operation` represents INSERT, DELETE, UPDATE_OLD, and UPDATE_NEW row changes.
