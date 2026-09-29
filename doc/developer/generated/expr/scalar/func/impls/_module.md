---
source: src/expr/src/scalar/func/impls.rs
revision: 3fbd0db9c8
---

# mz-expr::scalar::func::impls

Re-exports all type-specific scalar function implementations, organized into one submodule per SQL type.
Each submodule uses `#[sqlfunc]`-annotated functions or manual `LazyUnaryFunc`/`EagerUnaryFunc`/`EagerBinaryFunc` implementations that are collected into the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums by `macros.rs`.

Submodules: `array`, `boolean`, `byte`, `case_literal`, `catalog`, `char`, `date`, `datum`, `float32`, `float64`, `int16`, `int2vector`, `int32`, `int64`, `interval`, `jsonb`, `list`, `map`, `mz_acl_item`, `mz_timestamp`, `numeric`, `oid`, `pg_legacy_char`, `range`, `record`, `regproc`, `string`, `time`, `timestamp`, `uint16`, `uint32`, `uint64`, `uuid`, `varchar`. The `catalog` submodule contains thin `#[sqlfunc]`-annotated wrappers that delegate to `mz-catalog-decode` for decoding durable catalog data; these include `parse_catalog_id`, `parse_catalog_privileges`, `parse_catalog_acl_mode`, `parse_catalog_audit_log_details`, `parse_catalog_create_sql`, `parse_catalog_item_references`, `parse_postgres_source_details`, `parse_kafka_source_details`, `parse_source_export_details`, and `parse_connection_details`.
