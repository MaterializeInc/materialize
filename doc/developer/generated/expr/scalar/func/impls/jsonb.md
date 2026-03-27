---
source: src/expr/src/scalar/func/impls/jsonb.rs
revision: af9155582e
---

# mz-expr::scalar::func::impls::jsonb

Provides scalar function implementations for `jsonb` datums: subscript operators, `jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, type-coercion casts, `jsonb_build_object`, `jsonb_build_array`, `jsonb_pretty`, and containment checks.
Also provides `parse_catalog_id` (converts catalog JSON-serialized IDs into string format like `u1`, `s2`, `p`) and `parse_catalog_privileges` (converts catalog JSON-serialized privilege arrays into `mz_aclitem[]`), with helper functions `jsonb_datum_to_u64` and `jsonb_datum_to_role_id` for the conversions.
