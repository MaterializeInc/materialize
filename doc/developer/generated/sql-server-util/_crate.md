---
source: src/sql-server-util/src/lib.rs
revision: c2fc5e83f8
---

# mz-sql-server-util

SQL Server utility library that provides a connection client, CDC streaming, schema inspection, and row decoding for Materialize's SQL Server source.
The crate root defines `Client` (a higher-level wrapper over `tiberius` that models transactions via an internal request/response channel, exposing `execute`, `query`, `query_streaming`, `simple_query`, `transaction`, `set_transaction_isolation`, and `get_transaction_isolation`), `Transaction` (RAII guard with auto-rollback on drop; exposes `create_savepoint`, `lock_table_shared`, `get_lsn`, `commit`, and `rollback`), and `Connection` (the companion future that drives the underlying TCP+TDS connection via an `UnboundedReceiver` loop).
`TransactionIsolationLevel` enumerates the five SQL Server isolation levels (`ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Snapshot`, `Serializable`).
Error types `SqlServerError` and `SqlServerDecodeError` are defined at the crate root; `SqlServerDecodeError`'s string representation is durably stored in persist and must remain stable across releases.
`quote_identifier` is a public helper that bracket-quotes identifiers using `[` / `]` following SQL Server's `QUOTENAME` semantics.
`SqlServerCdcMetrics` is a trait for observing snapshot table lock start/end events, with a `LoggingSqlServerCdcMetrics` tracing-based implementation provided.
`config` holds connection and tunnel configuration (`TunnelConfig` supporting direct, SSH, and AWS PrivateLink connections); `desc` defines table/column descriptors and row decoding; `inspect` contains SQL queries against SQL Server system tables; `cdc` implements the `CdcStream` that orchestrates snapshots and change polling.
Key dependencies are `tiberius`, `mz-repr`, `mz-ssh-util`, and `mz-cloud-resources`; the `Lsn` type from the `cdc` module implements `timely::progress::Timestamp` for dataflow integration.
