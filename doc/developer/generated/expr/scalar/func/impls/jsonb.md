---
source: src/expr/src/scalar/func/impls/jsonb.rs
revision: 3fbd0db9c8
---

# mz-expr::scalar::func::impls::jsonb

Provides scalar function implementations for `jsonb` datums: subscript operators, `jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, type-coercion casts (`cast_jsonb_to_string`, `cast_jsonb_to_int16/32/64`, `cast_jsonb_to_float32/64`, `CastJsonbToNumeric`, `cast_jsonb_to_bool`, `cast_jsonbable_to_jsonb`), `jsonb_build_object`, `jsonb_build_array`, `jsonb_pretty`, and containment checks.
Catalog data decoding functions (`parse_catalog_id`, `parse_catalog_privileges`, `parse_catalog_acl_mode`, `parse_catalog_create_sql`, `parse_catalog_item_references`, `parse_postgres_source_details`, `parse_kafka_source_details`, `parse_source_export_details`, `parse_connection_details`, and `parse_catalog_audit_log_details`) live in the `catalog` sibling submodule, which delegates to `mz-catalog-decode`.
