---
source: src/transform/src/threshold_elision.rs
revision: ddc1ff8d2d
---

# mz-transform::threshold_elision

Implements `ThresholdElision`, which removes `Threshold` operators whose inputs are already known to have non-negative multiplicities.
It uses the `NonNegative` and `SubtreeSize` analyses to determine safety; `SubtreeSize` is used to avoid re-running the analysis unnecessarily on large subtrees.
