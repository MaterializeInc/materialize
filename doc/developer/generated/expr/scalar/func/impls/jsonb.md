---
source: src/expr/src/scalar/func/impls/jsonb.rs
revision: 190baa2a1b
---

# mz-expr::scalar::func::impls::jsonb

Provides scalar function implementations for `jsonb` datums: subscript operators, `jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, type-coercion casts, `jsonb_build_object`, `jsonb_build_array`, `jsonb_pretty`, and containment checks.
Also provides `parse_catalog_id` (converts catalog JSON-serialized IDs into string format like `u1`, `s2`, `p`), `parse_catalog_privileges` (converts catalog JSON-serialized privilege arrays into `mz_aclitem[]`), `parse_catalog_acl_mode` (converts a catalog JSON-serialized `AclMode` bitflags object of shape `{"bitflags": <u64>}` into a PostgreSQL ACL char-code string, e.g. `{"bitflags": 514}` → `"ar"`), and `parse_catalog_create_sql` (parses a catalog `create_sql` string into a JSONB object with type and additional fields depending on the statement: for connections, a `connection_type` field; for materialized views, `cluster_id` and `definition`; for sources (`CreateSource`), `source_type`, `cluster_id`, `connection_id`, `envelope_type`, `key_format`, and `value_format`; for webhook sources, `source_type` and `cluster_id`; for subsources, `source_type` and `of_source_id`), with helper functions `jsonb_datum_to_u64`, `jsonb_datum_to_acl_mode` (shared decoder for `AclMode` objects used by both `parse_catalog_privileges` and `parse_catalog_acl_mode`), and `jsonb_datum_to_role_id` for the conversions.
