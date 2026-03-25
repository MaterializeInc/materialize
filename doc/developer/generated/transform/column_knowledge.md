---
source: src/transform/src/column_knowledge.rs
revision: 703a0c27c8
---

# mz-transform::column_knowledge

Implements `ColumnKnowledge`, a transform that harvests per-column literal and nullability facts from filters and let-bindings and uses that knowledge to simplify scalar expressions throughout the plan.
It maintains a map from `Id` to per-column `DatumKnowledge` (which records whether a column is a known literal and its nullability), and propagates this information top-down while rewriting scalars bottom-up.
The `DatumKnowledge` type and its propagation logic are defined in this file alongside the transform.
