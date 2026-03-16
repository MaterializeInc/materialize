---
source: src/mysql-util/src/lib.rs
revision: e757b4d11b
---

# mysql-util

Provides utilities for connecting to MySQL databases, introspecting their schema, validating replication prerequisites, and decoding row data into Materialize `Row` values.
The crate is organized into six modules: `tunnel` (connection management with SSH/PrivateLink/direct transport and RDS IAM auth), `desc` (protobuf-serializable table/column/key descriptors), `schemas` (schema introspection via `information_schema`), `replication` (system-variable validation for CDC), `privileges` (source privilege checking), and `decoding` (`pack_mysql_row`).
The crate root also defines `MySqlError`, `UnsupportedDataType`, `MissingPrivilege`, `quote_identifier`, and error code constants.
Key dependencies are `mysql_async`, `mz-repr`, `mz-proto`, `mz-ssh-util`, and the AWS SDK crates.
Downstream consumers include `storage`, `storage-types`, `sql`, and `adapter`.
