---
source: src/expr/src/scalar/func/impls/audit_log_details.rs
revision: 8598d82c1c
---

# mz-expr::scalar::func::impls::audit_log_details

Implements the `parse_catalog_audit_log_details` SQL scalar function, which reshapes proto `audit_log_event_v1::Details` JSON (as stored in `mz_catalog_raw`) into the format `mz_audit_log::EventDetails::as_json` produces, so that `mz_audit_events` reads identically to what the prior `pack_audit_log_update` path wrote.

The reshape is driven by five static rule tables plus two structural rewrites:

- `FLATTENED_FIELDS`: handles `#[serde(flatten)]` sites by hoisting a nested sub-object's entries into the parent. Supports recursive sub-variant contexts (e.g. `AlterApplyReplacementV1.target` flattens an `IdFullNameV1` which itself flattens a `FullNameV1`).
- `RENAMES`: maps proto field names to their audit-log equivalents (e.g. `external_type` to `type`, `rehydration_time_estimate` to `hydration_time_estimate`).
- `DROP_NULL`: drops fields when null for variants where the audit-log side uses `#[serde(skip_serializing_if = "Option::is_none")]`.
- `ENUM_COLLAPSE`: collapses proto externally-tagged enums with `Empty` payloads into kebab strings (e.g. `{"reason": {"Manual": {}}}` to `"manual"`). Supports single-wrap (`{"K": {}}`) and double-wrap (`{"field": {"K": {}}}`) shapes.
- `DESCEND`: recurses into a nested field with a fresh variant context so that rules declared on the sub-variant fire correctly.

Two structural rewrites apply globally: `{"inner": v}` (`StringWrapper`) is unwrapped to `v`, and `ResetAllV1` maps to JSON null (the only variant whose `as_json` returns null rather than an object).

Rules are keyed on `(variant, path, field)` triples so that the same field name behaves differently under different variant contexts. Unlisted enum variants and non-empty `Empty` payloads produce an `EvalError::InvalidCatalogJson` error (fail-fast). The round-trip property test at `src/catalog/tests/audit_log_details.rs` samples every `Arbitrary` variant and catches drift when either side gains a new variant, field, or serde attribute.
