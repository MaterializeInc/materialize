---
source: src/repr/src/optimize.rs
revision: dab94d5cce
---

# mz-repr::optimize

Defines `OptimizerFeatureFlags`, a struct of boolean and integer feature flags that control optimizer behavior at query time, generated via the `optimizer_feature_flags!` macro and serializable for use in `EXPLAIN` output and session variables.
