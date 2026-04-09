---
source: src/storage/src/source/mysql.rs
revision: f498b6e141
---

# mz-storage::source::mysql

Implements `SourceRender` for `MySqlSourceConnection`, composing snapshot, replication, and statistics operators into a complete MySQL CDC ingestion dataflow.
The snapshot operator assigns each table to a specific worker, which performs a `SELECT * FROM table` and emits rewind requests containing the GTID-set frontier to the replication operator; the replication operator reads the binlog from a single worker using GTID-partitioned timestamps; the statistics operator probes the server to track offset progress and produces a probe stream.
A `schemas` submodule provides schema verification helpers used by the replication and snapshot operators to detect incompatible DDL changes.
Definite errors (bad column data) flow into the per-export error collection; transient errors (connection failures, SSH errors) trigger a dataflow restart via the health system.
