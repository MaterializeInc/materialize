---
source: src/sql/src/session/vars/definitions.rs
revision: e4df9977da
---

# mz-sql::session::vars::definitions

Defines `VarDefinition` (the static metadata for a variable: name, description, default value, constraints, feature-flag association, and `ParameterScope`) and declares all session and system variable definitions as `static` values.
`VarDefinition` carries a `scope: ParameterScope` field (from `mz_dyncfg`) indicating the scope at which the variable's value may be overridden by the LaunchDarkly sync loop. The `scoped(scope)` const builder method sets this field; the default is `ParameterScope::DEFAULT`. `VarDefinition` implements `Var::scope()` by returning this field.
The `feature_flags!` macro accepts an optional `scope: <expr>,` field per flag entry; when omitted the flag defaults to `ParameterScope::DEFAULT`. Cluster-coherent optimizer feature flags declare `scope: ParameterScope::Cluster`: these include `enable_new_outer_join_lowering`, `enable_eager_delta_joins`, `enable_left_joins_lowering`, `enable_variadic_left_join_lowering`, `enable_join_prioritize_arranged`, and `enable_projection_pushdown_after_relation_cse`.
The `lazy_value!` and `value!` macros (from `polyfill`) are used extensively to express default values that cannot be computed at compile time.
This file is the authoritative source of truth for which variables exist and their defaults.
`RESTRICT_TO_USER_OBJECTS` is a read-only `bool` session variable (default `false`) that restricts queries from accessing system catalog objects; it is designed to be set only via `ALTER ROLE ... SET` by superusers.
Feature flags include `enable_repeat_row_non_negative` (guards the `repeat_row_non_negative` table function), `enable_storage_introspection_logs` (guards forwarding storage timely logging events into the compute introspection dataflow), `enable_kafka_broker_matching_rules` (guards `MATCHING` broker rules in `BROKERS` for Kafka PrivateLink connections), `enable_glue_schema_registry` (guards `CREATE CONNECTION ... TO AWS GLUE SCHEMA REGISTRY`), `enable_bounded_staleness_isolation` (guards the `bounded staleness <duration>` transaction isolation level; defaults to `true`), `enable_fixed_correlated_cte_lowering` (gates the corrected HIR-to-MIR lowering path that uses CTE-aware branch keys when decorrelating CTEs referenced from nested correlated scopes; defaults to `true`), and `enable_statement_arrival_logging` (when enabled, SQL frontends log incoming statements at info level as they arrive, before processing, with literals redacted; intended as an emergency diagnostic for incidents where statements crash the process before reaching the statement log; defaults to `false`).
`DEFAULT_TIMESTAMP_INTERVAL`, `CLUSTER_CHECK_SCHEDULING_POLICIES_INTERVAL`, `SSH_CHECK_INTERVAL`, and `STORAGE_STATISTICS_INTERVAL` each carry a `NON_ZERO_DURATION` constraint, so setting any of them to zero raises an `InvalidParameterValue` error.
