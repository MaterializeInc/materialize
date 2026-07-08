# Dataflow visualizer: LIR grouping boxes

- Associated: [CNS-108](https://linear.app/materializeinc/issue/CNS-108), [CNS-109](https://linear.app/materializeinc/issue/CNS-109), builds on `20260706_dataflow_visualizer_rebuild.md`

## The Problem

The rebuilt dataflow visualizer (`20260706_dataflow_visualizer_rebuild.md`) shows LIR membership only as a badge, a tooltip on hover, and a side-panel tree.
Regions and LIR spans were deliberately kept as two different hierarchies: a region is a drill-down scope, a LIR span is an unrelated grouping over the same operators.
A user looking at a scope's operator graph cannot see at a glance which operators came from the same LIR node, which is exactly the grouping `EXPLAIN` output already draws (dashed boxes, one per LIR node, nested when LIR spans nest).
Matching that visual on the canvas would let a user correlate the rendered graph directly with the plan they already read.

## Success Criteria

* A user can toggle "Show LIR groups" in the toolbar and see, for the current scope, a dashed box around each LIR node's member operators, nested to match LIR span nesting.
* A box shows the LIR operator string as a header label.
* Clicking a box's header opens the same detail info the LIR side panel already shows for that LIR node (id, operator, member count, aggregated stats).
* Toggling off returns to exactly today's flat layout, byte-identical to the current behavior.
* Search, dimming, and heatmap decoration keep working unchanged on individual operators inside a box.

## Out of Scope

* Chains: gluing linear (in-degree 1, out-degree 1) operator runs into one visually merged box, a separate idea explored and shelved during this design's brainstorming as added complexity without a clean elk-native way to render it.
* Any change to how LIR data is queried or computed (`useDataflowGraphData.ts`, `mz_lir_mapping`) — grouping is a pure derivation over data already fetched.
* Heat-coloring or aggregate stats displayed directly on a group box; a box is a label-only wrapper, its members carry all stats as they do today.
* Drill-down or double-click behavior on a group box. LIR groups are not scopes and do not navigate.

## Solution Proposal

Add a pure grouping step between the existing decoration pass and the elk layout step, and teach the elk/React Flow rendering path to lay out and draw nested compound nodes, which it does not do today.

### Data model

A dataflow's direct children in a scope are already ordered by operator id, and each carries `lir: LirInfo[]`, an outer-to-inner nesting stack.
LIR spans are contiguous ranges over operator ids and properly nested: a region is always fully enclosed by a single LIR node's range, and a single region can contain operators from multiple LIR nodes, but always consecutive by operator id.
A new pure function in `dataflowGraph.ts`, `groupByLir(nodes: VisibleNode[]): LirGrouping`, exploits this with a single left-to-right scan and an explicit stack of open groups: for each node, pop groups whose `lirId` is no longer a prefix of the node's stack, push newly opened ones.
This is a bracket-matching walk, valid because of the contiguity and proper-nesting invariant above; it does not need to handle malformed input, since the invariant is guaranteed by how LIR spans are constructed.

Rather than replacing the flat node list with a nested tree, grouping is a separate layer over it, the same way `decorateGraph` layers dimming and color over an unchanged `VisibleGraph`: every downstream consumer (search, dimming, click handling) keeps working against the flat list untouched, and only the layout/render step reads the grouping layer.
Some children stay ungrouped (empty `lir`, e.g. ports, which carry no LIR data and are never absorbed into a group); every other child, and every group itself, has an immediate parent recorded in `parentOf`:

```typescript
interface LirGroupNode {
  id: NodeId; // `${exportId}/${lirId}`, matching lirIndex's existing key
  exportId: string;
  lirId: string;
  operator: string;
  nesting: number;
}

interface LirGrouping {
  groups: LirGroupNode[]; // outer groups before the inner groups nested in them
  parentOf: Map<NodeId, NodeId>; // a VisibleNode or LirGroupNode id -> its immediate enclosing group's id
}
```

A dataflow can back several exports, and an operator's `lir: LirInfo[]` can hold entries from more than one of them at once, when a shared subplan feeds multiple exports.
A compound-node tree needs a single parent chain per node, so `groupByLir` groups by one export only: the lowest `exportId` (string-compared) present among the scope's direct children.
A child whose only `lir` entries belong to a different export renders ungrouped in that scope, alongside children with no LIR data at all.
This can under-group a multi-export scope, but never draws two overlapping box hierarchies over the same operators.

### Rendering and layout

`elkGraph.ts`'s `toElkGraph` is flat-only today, by its own comment: a view is always one scope's direct children, nothing nests.
LIR groups are the first use of elk's native compound-node support in this codebase.
`toElkGraph` grows a case for `LirGroupNode`: it becomes an elk parent node with its members as elk children (recursing for nested groups), no fixed width or height, since elk auto-sizes a parent from its children plus `elk.padding`, reserving a header strip for the label.
Ungrouped siblings and group roots sit together as top-level elk children, exactly like today's flat list when there are no groups.
Edges can still cross group boundaries (an edge's endpoints are unrelated to which LIR group either side belongs to), so the root layout options need `elk.hierarchyHandling: INCLUDE_CHILDREN`, harmless when there are no groups at all.

React Flow's `parentId` mechanism (nested parent nodes, already used as one of the reasons the project chose React Flow) maps onto this directly, and needs no coordinate conversion: elk reports a child's `x`/`y` relative to its immediate parent's origin, the same convention React Flow uses for a node with `parentId` set.
A new `LirGroupNode` component renders the dashed border and a small header label (from `LirInfo.operator`, truncated with ellipsis like the existing LIR panel), colored by `lirId`, sized from elk's computed group bounds.
It carries no stats and is unaffected by heatmap coloring.

Pipeline order: `deriveVisibleGraph` → `decorateGraph` → (if the toggle is on) `groupByLir` → `toElkGraph` → layout → render.
Grouping is the last, purely structural step over already-decorated nodes, so search highlighting, dimming, and heatmap coloring apply to members exactly as they do without groups; a group box just visually contains whichever of its members are dimmed or highlighted.
With the toggle off, `deriveVisibleGraph`'s output goes straight to `toElkGraph`, unrouted through `groupByLir`, identical to today's behavior.

### Interactions

Clicking a group's header selects that LIR node and opens `NodeDetailPanel` with the same info a `LirPanel` tree entry already produces (`lirIndex`/`lirSummary`): id, operator string, member count, aggregated stats.
No double-click or drill-down action on a group; LIR groups are not scopes.

### Toolbar

A new "Show LIR groups" checkbox in `DataflowToolbar`, off by default, alongside the existing search/hide-idle/heatmap controls.
Off, the view is exactly today's flat grid.

## Minimal Viable Prototype

The risk worth de-risking before full build is elk's compound-node layout quality and cost on a real, multi-level LIR-nested dataflow, since this codebase has never exercised that path.
A thin slice: run `groupByLir` and the elk-nesting change against one scope of the reference dataflow from the original design doc (or another with visibly nested LIR spans), confirm cross-group edge routing looks reasonable and layout time stays acceptable, before building out the toolbar toggle, `LirGroupNode` styling, and the click-to-detail wiring.

## Alternatives

* Render-layer-only grouping: keep elk flat, compute each group's bounding box in the render layer from its members' already-laid-out positions, and draw an absolutely-positioned dashed `<Box>` behind them without going through elk's compound-node support or React Flow's `parentId`.
  Avoids touching `elkGraph.ts`, but duplicates layout knowledge elk already has (padding, header reservation, nested spacing) in a second place, and elk has no reason to leave room for a label strip or extra padding around a group it doesn't know exists, so boxes would collide with sibling groups or overlap edges routed through the gap.
* Always-on, no toggle.
  Simpler UI, but permanently changes the default look of every scope that has LIR data and adds elk layout cost (nested compound-node layout is slower than flat) to every view, including ones where the operator/LIR correlation isn't what the user is looking for.

## Open questions

* None blocking.
  Chains (gluing linear operator runs) were explored in the same brainstorming session and shelved; if revisited, see the discussion of why render-layer-only glue was rejected there, which mirrors the render-layer alternative rejected above.
