---
source: src/transform/src/demand.rs
revision: cc1d7672ff
---

# mz-transform::demand

Implements `Demand`, which propagates column-demand information from the root of a plan downward to operators, giving them permission to replace unused columns with dummy values.
This primarily benefits `Join`, which can drop columns from its intermediate state and reduce arrangement width.
The transform is largely superseded by `ProjectionPushdown` but still handles the specific case of rewriting join outputs that equate two demanded columns to reference a single column.
