---
source: src/repr/src/optimize.rs
revision: 38447d1ee0
---

# mz-repr::optimize

Defines `OptimizerFeatures`, a struct of boolean and integer feature flags that control optimizer behavior at query time, generated via the `optimizer_feature_flags\!` macro.
`OptimizerFeatureOverrides` is a companion struct where each field is `Option`-wrapped, allowing partial overrides to be layered on top of base features via the `OverrideFrom` trait.
`OverrideFrom<T>` is a generic trait for layered config construction; a blanket impl handles `Option<&T>` layers.
Both structs are serializable for use in `EXPLAIN` output, session variables, and catalog entries (e.g., `CREATE CLUSTER ... FEATURES(...)`), with bidirectional `BTreeMap<String, String>` conversions for persistence.
The `bool` implementation of the private `OptimizerFeatureType` trait decodes leniently: it accepts both Rust spellings (`"true"`/`"false"`) and the canonical system-var/PostgreSQL spellings (`"on"`/`"off"`) as well as `"1"`/`"0"`, `"y"`/`"n"`, and `"yes"`/`"no"`. This is necessary because cluster-coherent scoped overrides from LaunchDarkly arrive as the var's canonical `"on"`/`"off"` encoding. An unrecognized value logs a soft panic and falls back to `false` rather than crashing the optimizer on every plan for the affected cluster.
`OptimizerFeatures` includes `enable_fixed_correlated_cte_lowering: bool`, which gates the corrected HIR-to-MIR lowering path that uses CTE-aware branch keys when decorrelating CTEs referenced from nested correlated scopes.
`OptimizerFeatures` also includes `enable_union_cancellation_after_relation_cse: bool`, which gates the `UnionFusion` and `UnionBranchCancellation` passes that run in the physical optimizer after `RelationCSE` to cancel `A ∪ ¬A` branch pairs that become direct siblings only after literal lifting and CSE.
