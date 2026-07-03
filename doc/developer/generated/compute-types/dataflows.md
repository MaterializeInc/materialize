---
source: src/compute-types/src/dataflows.rs
revision: e926ec3a86
---

# compute-types::dataflows

Defines `DataflowDescription<P, S>`, the central descriptor for a compute dataflow: it lists source imports, index imports, objects to build (`BuildDesc<P>`), index exports, sink exports, as-of and until frontiers, refresh schedule, and time-dependence metadata.
`IndexDesc<E>` is now generic over the scalar expression type `E`; `IndexDesc<MirScalarExpr>` is used at the MIR level and carries a `as_lir()` method to translate to `IndexDesc<LirScalarExpr>`. `IndexImport` carries the import-side metadata with `desc: IndexDesc<MirScalarExpr>`; `SourceImport` carries source-instance description, monotonicity flag, snapshot requirement, and initial upper frontier.
`depends_on` and `depends_on_imports` are implemented on `DataflowDescription<P, S> where P: CollectionPlan` to compute transitive dependencies; `DataflowDescription::is_single_time` tests whether the dataflow covers exactly one timestamp. Helper iterators `materialized_view_ids`, `subscribe_ids`, and `copy_to_ids` enumerate exports by connection type.
`compatible_with` (on `DataflowDescription<RenderPlan, S>`) checks structural equality of two descriptions for command reconciliation purposes.
This type is generic over the plan representation `P`, allowing the same descriptor to hold MIR (`OptimizedMirRelationExpr`) before lowering and LIR (`LirRelationExpr`) after.
