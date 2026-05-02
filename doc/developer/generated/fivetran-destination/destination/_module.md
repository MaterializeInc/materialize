---
source: src/fivetran-destination/src/destination.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::destination

Provides the `MaterializeDestination` struct, which implements the `DestinationConnector` gRPC trait for the Fivetran SDK.
All gRPC handler methods delegate to child modules: `config` for connection/test, `ddl` for schema operations, and `dml` for data operations.
Responses are always HTTP 200 with embedded success/warning/error payloads, and transient failures are automatically retried up to 3 times via `with_retry_and_logging`.
`ColumnMetadata` is a shared helper capturing a column's escaped name, SQL type, and primary-key status.
