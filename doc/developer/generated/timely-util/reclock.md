---
source: src/timely-util/src/reclock.rs
revision: 9a84574e68
---

# timely-util::reclock

Implements the reclocking operator, which translates a source collection evolving under `FromTime` into one evolving under `IntoTime` using a remap collection `R`.
Defines `ReclockOperator` and supporting traits (`ReclockFollower`, `RemapOperator`, `RemapHandle`) that together track which source timestamps have been assigned `IntoTime` values and emit downstream updates accordingly.
The module contains detailed mathematical notation in its doc comment describing the formal semantics of reclocking as a frontier-bounded summation of source diffs.
The remap trace is stored as a `VecDeque` sorted by `IntoTime`; compaction exploits the total-order property of `IntoTime` to process only the prefix of entries whose timestamps fall below the advancing since frontier, avoiding a full sort on every compaction step.
The output capability is disconnected from the remap input (`new_output_connection([])`) so that the operator can drop its output capability and eagerly advance the output frontier to empty as soon as the source frontier is empty and all pending data has been reclocked, independent of how far the remap collection has advanced.
