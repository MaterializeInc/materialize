---
source: src/mz-deploy/src/client/auto_scaling.rs
revision: 3f3bdb0535
---

# mz-deploy::client::auto_scaling

Conversions between the `AUTO SCALING STRATEGY` cluster option and the structured `AutoScalingStrategy` policy.

Three representations meet here:

- The SQL option in a `CREATE CLUSTER` / `ALTER CLUSTER` statement (`ClusterAutoScalingStrategyOptionValue` AST).
- The structured policy (`mz_sql::plan::AutoScalingStrategy`), used for drift comparison.
- The `strategy` jsonb column of `mz_internal.mz_cluster_auto_scaling_strategies`, whose serde shape matches the plan type.

Key functions:

- `strategy_from_option_value` — converts an AST option value into a structured policy. An empty block `AUTO SCALING STRATEGY = ()` maps to `None` (autoscaling disabled).
- `strategy_to_cluster_option` — renders a structured policy back into a `CREATE CLUSTER` / `ALTER CLUSTER ... SET` option.
- `strategy_from_catalog_json` — parses the `strategy` jsonb column. A JSON `null` value maps to `None`, matching a cluster whose policy was just removed while a burst still lingers.
