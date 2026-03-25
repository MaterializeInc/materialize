---
source: src/compute-types/src/dataflows.rs
revision: 52af3ba2a1
---

# compute-types::dataflows

Defines `DataflowDescription<P, S, T>`, the central descriptor for a compute dataflow: it lists source imports, index imports, objects to build (`BuildDesc<P>`), index exports, sink exports, as-of and until frontiers, refresh schedule, and time-dependence metadata.
`IndexDesc` captures the key expressions and relation type for an index export, and `IndexImport` carries the import-side metadata.
`CollectionPlan` is implemented on `DataflowDescription<MirRelationExpr>` to support plan analysis; `DataflowDescription::is_single_time` tests whether the dataflow covers exactly one timestamp.
This type is generic over the plan representation `P`, allowing the same descriptor to hold MIR (`MirRelationExpr`) before lowering and LIR (`Plan`) after.
