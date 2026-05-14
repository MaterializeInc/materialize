---
source: src/sql/src/session/vars/definitions.rs
revision: 3df8ae2fd8
---

# mz-sql::session::vars::definitions

Defines `VarDefinition` (the static metadata for a variable: name, description, default value, constraints, feature-flag association) and declares all session and system variable definitions as `static` values.
The `lazy_value!` and `value!` macros (from `polyfill`) are used extensively to express default values that cannot be computed at compile time.
This file is the authoritative source of truth for which variables exist and their defaults.
`RESTRICT_TO_USER_OBJECTS` is a read-only `bool` session variable (default `false`) that restricts queries from accessing system catalog objects; it is designed to be set only via `ALTER ROLE ... SET` by superusers.
Feature flags include `enable_repeat_row_non_negative` (guards the `repeat_row_non_negative` table function), `enable_storage_introspection_logs` (guards forwarding storage timely logging events into the compute introspection dataflow), and `enable_kafka_broker_matching_rules` (guards `MATCHING` broker rules in `BROKERS` for Kafka PrivateLink connections).
