# Dataflow visualizer rebuild

- Associated: [CNS-108](https://linear.app/materializeinc/issue/CNS-108), [CNS-109](https://linear.app/materializeinc/issue/CNS-109)

## The Problem

The dataflow visualizer (`src/platform/clusters/DataflowVisualizer.tsx`) builds a Graphviz DOT string by hand and renders it with `d3-graphviz`, which pulls in a WASM Graphviz build.
This design has produced two open bugs.
CNS-108: `scopeToGv` concatenates catalog strings into DOT and only escapes double quotes, allowing XSS through crafted object names.
CNS-109: `useSqlApiRequest` never clears previous results, so a failed refetch leaves the stale graph on screen.
Beyond the bugs, the visualizer lacks features operators need: no dataflow selection (one object, one dataflow), no whole-graph overview (drill-down replaces the canvas), no filtering, and no way to find hotspots in large dataflows.

## Success Criteria

* A user can pick any dataflow running on a replica, including transient ones, and see its operator graph.
* Regions expand and collapse in place, with aggregated stats and rerouted edges on collapsed regions.
* Nodes show scheduling time and arrangement size, edges show message counts and container type.
* Filter tools let a user locate operators by name, hide idle elements, heat-color by metric, and filter edges by container type.
* Rendering strings from the catalog cannot inject markup (closes CNS-108 by construction).
* A failed refetch always replaces the graph with an error state (closes CNS-109, with a regression test).
* No `d3-graphviz` or WASM dependency remains.

## Out of Scope

* Backend changes.
  All required data exists in `mz_introspection` relations already queried today.
* Live-updating stats via `SUBSCRIBE`.
  The data layer separates structure from stats so this can be added later, but v1 ships manual refresh only.
* Per-worker breakdowns and skew analysis.
  V1 uses the worker-aggregated introspection views, as today.
* URL-persisted filter and collapse state.
* New end-to-end tests.

## Solution Proposal

Rebuild the visualizer on React Flow (`@xyflow/react`) for rendering and interaction, with elkjs (`elk.layered`) for hierarchical layout in a web worker.
Nodes become React components styled with Chakra, so all catalog strings render as text nodes and the DOT injection class disappears.
React Flow provides pan, zoom, minimap, selection, nested parent nodes, and viewport culling (`onlyRenderVisibleElements`), which matters for dataflows with over a thousand operators.
elkjs is the only mainstream JS layout engine with real compound-graph support: nested regions and edges routed across hierarchy boundaries.
elkjs is GWT-compiled JavaScript, not WASM.

### Product surface

Two entry points share one graph implementation.

* A new "Dataflows" tab on cluster detail: replica selector, then a dataflow list from `mz_dataflows` joined with `mz_records_per_dataflow` and summed `mz_scheduling_elapsed` (name, records, size, elapsed). Clicking a row opens the graph view. This covers transient dataflows such as peeks and subscribes.
* The existing "Visualize" tab on index and materialized view detail deep-links into the cluster page with the object's dataflow preselected, resolved via `mz_compute_exports`.

Both remain behind the `visualization-features` LaunchDarkly flag.
The old `DataflowVisualizer.tsx` and the `d3-graphviz` dependency are deleted once the new view lands, since the visualizer is its only consumer.

### Data layer

The three structure queries from `useDataflowStructure.ts` (operators, channels, LIR operators) are kept, but the operator and channel queries are keyed by dataflow id instead of export id so transient dataflows work.
`mz_lir_mapping` is keyed by `global_id`, so LIR data exists only for dataflows backing an export.
For transient dataflows the LIR panel and badges are hidden.
A new cheap query feeds the dataflow selector list.

A new hook returns results tagged with the parameters that produced them.
The render layer discards results whose tag does not match the current selection, and an error always wins over retained data.
This closes CNS-109 without depending on the broken `requestIdRef` logic in `useSqlApiRequest`.

Refresh is manual: a refresh button plus a "last fetched" timestamp.
Layout, collapse state, and viewport survive a refresh.
Stats update in place, and relayout happens only if the structure changed.
The structure/stats split makes a later `SUBSCRIBE`-driven live mode a drop-in: stats stream into node badges without relayout.

### Graph model and collapse

Pure functions in a new `dataflowGraph.ts` build the region tree from `mz_dataflow_addresses` and `mz_dataflow_operator_parents`, replacing `collateOperators`.
Regions map to React Flow parent (group) nodes, operators to leaf nodes.
Every node carries own and transitive stats: arrangement records, size, elapsed.

Each region is expanded or collapsed in place.
A collapsed region renders as a single node with aggregated stats and a child count.
Edges crossing a collapsed boundary are remapped to the region node and aggregated: one edge per direction pair, summed records and batches, and the set of container types.
The derivation is a pure function from `(structure, collapseState)` to visible nodes and edges.
The default state shows the root's direct children with everything below collapsed.

Regions and LIR spans are two different hierarchies, so only regions get the nesting.
LIR information appears as a badge and operator text on each node, plus a side panel listing the dataflow's LIR operators from `mz_lir_mapping`.
Hovering or clicking a panel entry highlights the member operators on the canvas.
Scope inputs and outputs stay as small port nodes on the region boundary, as today.

```mermaid
flowchart LR
    sql[SQL results] --> structure[dataflowGraph.ts: region tree + stats]
    structure --> visible["(structure, collapseState, filters) -> visible nodes/edges"]
    visible --> elk[elkjs worker layout]
    elk --> rf[React Flow canvas]
    rf -- expand/collapse, filter --> visible
```

### Rendering and layout

elkjs runs `elk.layered` in a web worker, direction left to right, over exactly the visible post-collapse graph.
Toggling collapse recomputes layout, memoized per collapse state so toggling back is instant.
A small "layouting" overlay shows during computation while the canvas stays interactive.

The node component shows name, arranged records, size, and elapsed time.
Default colors keep today's two-by-two palette (region versus operator, has arrangement versus not).
Heatmap mode overrides node color.
Edges are labeled with records and batches, dashed when zero messages were sent, with the container type as an edge badge and tooltip.

Perf guards: collapse-by-default keeps the initial node count small, viewport culling handles large expanded graphs, and if the visible node count would exceed roughly 1500 the UI warns and refuses expand-all.

### Filters and interactions

A toolbar above the canvas offers:

* Name search: matching operators highlighted, others dimmed, next/previous jump that auto-expands ancestor regions of a match.
* Hide idle: hides zero-message edges and zero-elapsed operators, with region aggregates recomputed over the visible set.
* Heatmap: mode select (off, elapsed, arrangement size), sequential color scale, threshold slider that dims nodes below the cutoff, with a legend.
* Channel type: multi-select over the container types present, non-matching edges dimmed.

Filters are pure derivations over the visible graph and compose.
Filter state lives in component state.
Clicking a node opens a detail side panel (full name, all stats, LIR operator, address).
Double-clicking a region toggles collapse.

### Error handling

An error always replaces the graph.
The CNS-109 regression test from the issue lands, adapted to the new component.
`INSUFFICIENT_PRIVILEGE` keeps the existing USAGE-privilege alert.
A timeout shows an error box with retry.
A replica or dataflow that disappeared on refresh shows a "dataflow no longer exists" empty state and refreshes the selector.

## Minimal Viable Prototype

The riskiest assumption is elkjs layout quality and speed on real dataflow shapes.
Phase 1 of the implementation plan is a thin vertical slice: fetch one dataflow, build the region tree, lay out the default collapsed view with elkjs, and render it in React Flow with expand/collapse only.
That slice runs against a local environmentd with a non-trivial dataflow (for example a multi-way join with reductions) and validates layout, before filters and the selector page are built.

## Alternatives

* Extend the in-house `src/components/Graph` Canvas plus dagre stack (used by WorkflowGraph, RoleGraph, CriticalPathGraph).
  No new dependencies, but dagre's compound-graph support is weak, in-place collapse and boundary edge rerouting would be hand-built, and the plain SVG canvas has no viewport culling for graphs with over a thousand nodes.
  The hard part of this feature is exactly the part dagre is bad at.
* In-house Canvas rendering with elkjs layout.
  Keeps console rendering conventions and solves hierarchy, but rebuilds interaction machinery React Flow ships (nested nodes, culling, minimap, selection).
* Keep Graphviz but sanitize DOT properly.
  Fixes CNS-108 but keeps the WASM dependency, string-based pipeline, and none of the requested interaction features.

## Open questions

* None blocking.
  The `SUBSCRIBE` live mode and per-worker views are deliberate follow-ups, not open design points.

## Testing

* Unit (vitest): `dataflowGraph.ts` pure functions.
  Region tree construction, collapse edge remapping and aggregation, filter derivations, LIR membership.
  Most logic lives here, so most coverage lands here.
* Component (vitest plus msw): selector flow, error-on-refetch (the CNS-109 case), permission alert, empty states.
  React Flow renders with the elk worker stubbed to deterministic positions.
