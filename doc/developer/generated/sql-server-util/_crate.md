---
source: src/sql-server-util/src/lib.rs
revision: 1b337d84b0
---

# mz-sql-server-util

SQL Server utility library that provides a connection client, CDC streaming, schema inspection, and row decoding for Materialize's SQL Server source.
The crate root defines `Client` (a higher-level wrapper over `tiberius` that models transactions via an internal request/response channel) and `Transaction` (RAII transaction with auto-rollback on drop); `Connection` is the companion future that drives the underlying TCP+TDS connection.
`config` holds connection and tunnel configuration (`TunnelConfig` supporting direct, SSH, and AWS PrivateLink connections); `desc` defines table/column descriptors and row decoding; `inspect` contains SQL queries against SQL Server system tables; `cdc` implements the `CdcStream` that orchestrates snapshots and change polling.
Key dependencies are `tiberius`, `mz-repr`, `mz-ssh-util`, and `mz-cloud-resources`; the `Lsn` type from the `cdc` module implements `timely::progress::Timestamp` for dataflow integration.
