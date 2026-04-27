---
source: src/sql/src/session/vars/definitions.rs
revision: 44a09cff14
---

# mz-sql::session::vars::definitions

Defines `VarDefinition` (the static metadata for a variable: name, description, default value, constraints, feature-flag association) and declares all session and system variable definitions as `static` values.
The `lazy_value!` and `value!` macros (from `polyfill`) are used extensively to express default values that cannot be computed at compile time.
This file is the authoritative source of truth for which variables exist and their defaults.
Feature flags include `enable_repeat_row_non_negative` (guards the `repeat_row_non_negative` table function) and `enable_storage_introspection_logs` (guards forwarding storage timely logging events into the compute introspection dataflow).
