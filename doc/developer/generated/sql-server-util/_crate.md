---
source: src/sql-server-util/src/lib.rs
revision: b3a16a93da
---

# mz-sql-server-util

SQL Server utility library that provides a connection client, CDC streaming, schema inspection, and row decoding for Materialize's SQL Server source.
The crate root defines `Client` (a higher-level wrapper over `tiberius` that models transactions) and `Transaction` (RAII transaction with auto-rollback on drop); `Connection` is the companion future that drives the underlying TCP+TDS connection.
`config` holds connection and tunnel configuration; `desc` defines table/column descriptors and row decoding; `inspect` contains SQL queries against SQL Server system tables; `cdc` implements the `CdcStream` that orchestrates snapshots and change polling.
Key dependencies are `tiberius`, `timely`, `mz-repr`, `mz-ssh-util`, and `mz-cloud-resources`; the `Lsn` type from the `cdc` module implements `timely::progress::Timestamp` for dataflow integration.
