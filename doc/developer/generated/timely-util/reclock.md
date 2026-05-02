---
source: src/timely-util/src/reclock.rs
revision: b0fa98e931
---

# timely-util::reclock

Implements the reclocking operator, which translates a source collection evolving under `FromTime` into one evolving under `IntoTime` using a remap collection `R`.
Defines `ReclockOperator` and supporting traits (`ReclockFollower`, `RemapOperator`, `RemapHandle`) that together track which source timestamps have been assigned `IntoTime` values and emit downstream updates accordingly.
The module contains detailed mathematical notation in its doc comment describing the formal semantics of reclocking as a frontier-bounded summation of source diffs.
