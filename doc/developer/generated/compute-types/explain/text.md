---
source: src/compute-types/src/explain/text.rs
revision: e926ec3a86
---

# compute-types::explain::text

Implements `DisplayText` for `LirRelationExpr` nodes, rendering the LIR plan tree in the `EXPLAIN AS TEXT` format.
Each `LirRelationExpr` variant maps to an uppercase header line; sub-plan details follow as indented `key=value` pairs or lowercase field lines.
Supports both a default and a `verbose_syntax` mode; the latter is kept in sync with the `Display` implementation for `RenderPlanExprHumanizer`.
Delegates join, reduce, threshold, and top-k sub-plan formatting to helper impls defined in the same file.
Join nodes are labeled `→Differential Cross Join` or `→Delta Cross Join` when any stage is a cross product (empty lookup key), and `→Differential Join` or `→Delta Join` otherwise. The private `fmt_join_chain` helper formats the join chain as `%pos:name[key]` entries, annotating each input with its relation name (if the underlying `LirRelationNode::Get` can be identified) and key columns.
`Reduce` and `TopK` nodes whose `temporal_bucketing_strategy` is `TemporalBucketing` are prefixed with `Temporally-Bucketed ` in the output (e.g., `→Temporally-Bucketed Accumulable GroupAggregate`). A `Union` node where any input uses `TemporalBucketing` is similarly labeled `Temporally-Bucketed Union`. The verbose mode also emits a `temporal_bucketing_strategy=<value>` line for `ArrangeBy` nodes whose strategy is non-`Direct`.
