---
source: src/fivetran-destination/src/utils.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::utils

Provides type-mapping utilities and async I/O adapters shared across the destination.
`to_materialize_type` maps Fivetran `DataType` values to Materialize SQL type names; `to_fivetran_type` performs the reverse mapping from `mz_pgrepr::Type`.
`AsyncCsvReaderTableAdapter` re-maps CSV column order to match the destination table's column order, and `CopyIntoAsyncWrite` adapts a `tokio_postgres::CopyInSink` to implement `AsyncWrite`.
