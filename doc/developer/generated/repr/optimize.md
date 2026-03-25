---
source: src/repr/src/optimize.rs
revision: de1872534e
---

# mz-repr::optimize

Defines `OptimizerFeatures`, a struct of boolean and integer feature flags that control optimizer behavior at query time, generated via the `optimizer_feature_flags\!` macro.
`OptimizerFeatureOverrides` is a companion struct where each field is `Option`-wrapped, allowing partial overrides to be layered on top of base features via the `OverrideFrom` trait.
`OverrideFrom<T>` is a generic trait for layered config construction; blanket impls handle `Option<&T>` and `&[T]` layers.
Both structs are serializable for use in `EXPLAIN` output, session variables, and catalog entries (e.g., `CREATE CLUSTER ... FEATURES(...)`), with bidirectional `BTreeMap<String, String>` conversions for persistence.
