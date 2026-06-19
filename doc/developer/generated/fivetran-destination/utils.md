---
source: src/fivetran-destination/src/utils.rs
revision: 849327076c
---

# mz-fivetran-destination::utils

Shared utilities for the Fivetran destination connector.

`is_system_column(name)` — returns `true` if `name` starts with `_fivetran_`, the prefix Fivetran uses for its system columns.

`to_materialize_type(ty: DataType)` — maps a Fivetran SDK `DataType` to the corresponding Materialize SQL type name string (e.g. `DataType::Boolean` → `"boolean"`, `DataType::Long` → `"bigint"`, `DataType::Json` → `"jsonb"`). Returns `OpError` with `OpErrorKind::Unsupported` for unmapped types.

`AsyncCsvReaderTableAdapter` — a `Stream<Item = ByteRecord>` adapter that reads CSV rows from an `AsyncRead`, maps columns by header name against a Fivetran `Table` definition, and yields `ByteRecord`s in the table's column order.
