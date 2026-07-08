# Dataflow LIR Grouping Boxes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Draw nested dashed boxes around a scope's operators that share a LIR node, toggled from the dataflow visualizer toolbar, matching `EXPLAIN`'s own LIR grouping.

**Architecture:** A new pure function `groupByLir` derives a grouping layer (which group each `VisibleNode` sits in, if any) over the already-flat `VisibleGraph`, the same way `decorateGraph` layers dimming/color over it without replacing it. `elkGraph.ts` turns that layer into elk's native compound-node nesting (the first use of it in this codebase); `DataflowGraphView.tsx` turns elk's result into React Flow `parentId` nesting and a new `LirGroupNode` render component. A toolbar switch and a `NodeDetailPanel` case round out the feature.

**Tech Stack:** TypeScript, React, `@xyflow/react` (React Flow), `elkjs`, Chakra UI, Vitest.

## Global Constraints

* Design doc: `console/doc/design/20260707_dataflow_lir_grouping_boxes.md`. Every task below implements a specific section of it; read it first if anything here is ambiguous.
* Toggle defaults to off (`Filters.showLirGroups: false`); with it off, output must be byte-identical to today's flat layout.
* A node whose `lir` entries all belong to an export other than the scope's chosen export (lowest `exportId` present) renders ungrouped, never in two overlapping group hierarchies.
* No drill-down/double-click behavior on a group box; groups are label-only wrappers, never scopes.
* Run `cd console && yarn typecheck && yarn lint && yarn test <touched files>` before every commit in this plan (see `mz-run`/`mz-test` conventions); this plan's own commands below are the minimum, not a substitute.
* Copy comments verbatim where this plan quotes them from the design doc or existing code; don't invent new phrasing for an already-settled explanation.

---

### Task 1: `groupByLir` pure function

**Files:**
- Modify: `console/src/platform/dataflows/dataflowGraph.ts`
- Test: `console/src/platform/dataflows/dataflowGraph.test.ts`

**Interfaces:**
- Consumes: `VisibleNode` (already defined in this file: `id`, `operatorId: bigint | null`, `lir: LirInfo[]`).
- Produces:
  ```typescript
  export interface LirGroupNode {
    id: NodeId; // `${exportId}/${lirId}`, matching lirIndex's existing key
    exportId: string;
    lirId: string;
    operator: string;
    nesting: number;
  }
  export interface LirGrouping {
    groups: LirGroupNode[]; // outer groups appear before groups nested inside them
    parentOf: Map<NodeId, NodeId>; // a VisibleNode or LirGroupNode id -> its immediate enclosing group's id
  }
  export function groupByLir(nodes: readonly VisibleNode[]): LirGrouping
  ```
  Later tasks import `groupByLir`, `LirGrouping`, `LirGroupNode` from this file.

- [ ] **Step 1: Write the failing tests**

Add to `console/src/platform/dataflows/dataflowGraph.test.ts`, after the existing `import` block add `type LirInfo` and `groupByLir` to the named imports from `./dataflowGraph`, then append this new `describe` block at the end of the file:

```typescript
describe("groupByLir", () => {
  const node = (
    id: string,
    operatorId: number,
    lir: LirInfo[] = [],
  ): VisibleNode => ({
    id,
    kind: "operator",
    label: id,
    stats: null,
    transitive: null,
    transitiveSkew: null,
    childCount: 0,
    lir,
    address: null,
    operatorId: BigInt(operatorId),
    peers: [],
  });
  const port = (id: string): VisibleNode => ({
    id,
    kind: "port",
    label: id,
    stats: null,
    transitive: null,
    transitiveSkew: null,
    childCount: 0,
    lir: [],
    address: null,
    operatorId: null,
    peers: [],
  });
  const span = (
    exportId: string,
    lirId: string,
    nesting: number,
    parentLirId: string | null,
  ): LirInfo => ({ exportId, lirId, parentLirId, nesting, operator: lirId });

  it("groups nothing when no node has lir data", () => {
    const g = groupByLir([node("a", 1), node("b", 2)]);
    expect(g.groups).toEqual([]);
    expect(g.parentOf.size).toEqual(0);
  });

  it("groups a flat run under one lir id", () => {
    const s = span("u1", "10", 0, null);
    const g = groupByLir([node("a", 1, [s]), node("b", 2, [s]), node("c", 3, [s])]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10"]);
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.get("b")).toEqual("u1/10");
    expect(g.parentOf.get("c")).toEqual("u1/10");
  });

  it("nests an inner span inside an outer span", () => {
    const outer = span("u1", "10", 0, null);
    const inner = span("u1", "11", 1, "10");
    const g = groupByLir([
      node("a", 1, [outer]),
      node("b", 2, [outer, inner]),
      node("c", 3, [outer, inner]),
      node("d", 4, [outer]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10", "u1/11"]);
    expect(g.parentOf.get("u1/11")).toEqual("u1/10");
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.get("b")).toEqual("u1/11");
    expect(g.parentOf.get("c")).toEqual("u1/11");
    expect(g.parentOf.get("d")).toEqual("u1/10");
  });

  it("leaves ports and lir-less nodes ungrouped alongside a group", () => {
    const s = span("u1", "10", 0, null);
    const g = groupByLir([node("a", 1, [s]), port("p"), node("b", 2)]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10"]);
    expect(g.parentOf.get("a")).toEqual("u1/10");
    expect(g.parentOf.has("p")).toEqual(false);
    expect(g.parentOf.has("b")).toEqual(false);
  });

  it("picks the lowest exportId, leaving other-export-only nodes ungrouped", () => {
    const sA = span("u1", "5", 0, null);
    const sB = span("u2", "3", 0, null);
    const g = groupByLir([node("a", 1, [sA]), node("b", 2, [sB]), node("c", 3, [sA, sB])]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/5"]);
    expect(g.parentOf.get("a")).toEqual("u1/5");
    expect(g.parentOf.has("b")).toEqual(false);
    expect(g.parentOf.get("c")).toEqual("u1/5");
  });

  it("sorts input by operatorId regardless of array order", () => {
    const outer = span("u1", "10", 0, null);
    const inner = span("u1", "11", 1, "10");
    // Deliberately out of operatorId order.
    const g = groupByLir([
      node("d", 4, [outer]),
      node("b", 2, [outer, inner]),
      node("a", 1, [outer]),
      node("c", 3, [outer, inner]),
    ]);
    expect(g.groups.map((x) => x.id)).toEqual(["u1/10", "u1/11"]);
    expect(g.parentOf.get("b")).toEqual("u1/11");
    expect(g.parentOf.get("c")).toEqual("u1/11");
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd console && yarn vitest run src/platform/dataflows/dataflowGraph.test.ts`
Expected: FAIL with `groupByLir is not exported` / `groupByLir is not a function`.

- [ ] **Step 3: Implement `groupByLir`**

Add to `console/src/platform/dataflows/dataflowGraph.ts`, after `lirTree` (end of file):

```typescript
export interface LirGroupNode {
  id: NodeId; // `${exportId}/${lirId}`, matching lirIndex's key
  exportId: string;
  lirId: string;
  operator: string;
  nesting: number;
}

// A grouping layer over an already-derived VisibleGraph, the same way
// decorateGraph layers dimming and color over it: nothing here replaces
// `nodes`, so search, dimming, and click handling keep working against the
// flat list untouched.
export interface LirGrouping {
  groups: LirGroupNode[]; // outer groups appear before groups nested inside them
  parentOf: Map<NodeId, NodeId>; // a VisibleNode or LirGroupNode id -> its immediate enclosing group's id
}

// Groups a scope's direct children into the LIR tree that encloses them, for
// drawing nested boxes on the canvas. LIR spans are contiguous ranges over
// operator ids and properly nested: a region is always fully enclosed by a
// single LIR node's range. A dataflow can back several exports, and a shared
// subplan can carry lir entries from more than one of them at once; a
// compound-node tree needs one parent chain per node, so this groups by the
// lowest exportId (string-compared) present among the given nodes only. A
// node whose only lir entries belong to a different export renders
// ungrouped, alongside nodes with no lir data at all (e.g. ports).
export function groupByLir(nodes: readonly VisibleNode[]): LirGrouping {
  const groups: LirGroupNode[] = [];
  const parentOf = new Map<NodeId, NodeId>();
  const ordered = nodes
    .filter((n) => n.operatorId !== null)
    .slice()
    .sort((a, b) => {
      const x = a.operatorId!;
      const y = b.operatorId!;
      return x < y ? -1 : x > y ? 1 : 0;
    });
  if (ordered.length === 0) return { groups, parentOf };

  let chosenExport: string | null = null;
  for (const n of ordered) {
    for (const l of n.lir) {
      if (chosenExport === null || l.exportId < chosenExport) {
        chosenExport = l.exportId;
      }
    }
  }
  if (chosenExport === null) return { groups, parentOf };

  const groupIndex = new Map<string, LirGroupNode>();
  const stack: LirGroupNode[] = []; // open groups, outer to inner
  for (const node of ordered) {
    const stackKeys = node.lir
      .filter((l) => l.exportId === chosenExport)
      .sort((a, b) => a.nesting - b.nesting)
      .map((l) => `${l.exportId}/${l.lirId}`);
    let commonLen = 0;
    while (
      commonLen < stack.length &&
      commonLen < stackKeys.length &&
      stack[commonLen].id === stackKeys[commonLen]
    ) {
      commonLen++;
    }
    stack.length = commonLen; // pop back to the shared prefix
    for (let depth = commonLen; depth < stackKeys.length; depth++) {
      const key = stackKeys[depth];
      let group = groupIndex.get(key);
      if (!group) {
        const info = node.lir.find((l) => `${l.exportId}/${l.lirId}` === key)!;
        group = {
          id: key,
          exportId: info.exportId,
          lirId: info.lirId,
          operator: info.operator,
          nesting: info.nesting,
        };
        groupIndex.set(key, group);
        groups.push(group);
      }
      if (depth > 0) parentOf.set(group.id, stack[depth - 1].id);
      stack.push(group);
    }
    if (stack.length > 0) parentOf.set(node.id, stack[stack.length - 1].id);
  }
  return { groups, parentOf };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd console && yarn vitest run src/platform/dataflows/dataflowGraph.test.ts`
Expected: PASS, all tests including the six new `groupByLir` cases.

- [ ] **Step 5: Typecheck and lint**

Run: `cd console && yarn typecheck && yarn lint`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add console/src/platform/dataflows/dataflowGraph.ts console/src/platform/dataflows/dataflowGraph.test.ts
git commit -m "$(cat <<'EOF'
console: add groupByLir for LIR-node grouping boxes

Pure derivation over a scope's already-derived VisibleGraph, mirroring
decorateGraph's layering rather than replacing the flat node list, so
downstream search/dimming/click handling is untouched.
EOF
)"
```

---

### Task 2: elk nested compound-node layout

**Files:**
- Modify: `console/src/platform/dataflows/elkGraph.ts`
- Modify: `console/src/platform/dataflows/useElkLayout.ts`
- Test: `console/src/platform/dataflows/elkGraph.test.ts`

**Interfaces:**
- Consumes: `LirGrouping`, `LirGroupNode` from Task 1's `dataflowGraph.ts`.
- Produces: `toElkGraph(graph: VisibleGraph, grouping?: LirGrouping): ElkNode` (grouping now optional third-party consumers can omit), `extractPositions` unchanged in signature but now recurses into nested `children`. `useElkLayout(graph, cacheKey, grouping?)` gains an optional third parameter.

- [ ] **Step 1: Write the failing tests**

Add to `console/src/platform/dataflows/elkGraph.test.ts`. First, add `groupByLir` to the import from `./dataflowGraph`, and add this import at the top (a real, synchronous elk instance — not the worker-backed `elk-api` used at runtime — for the empirical round-trip test):

```typescript
import ELK from "elkjs";
```

Then append:

```typescript
describe("toElkGraph with a LIR grouping", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("nests a group's members as elk children, sized only by content", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    const group = elk.children!.find((c) => c.id === grouping.groups[0].id)!;
    expect(group.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([nodeIdOf([5, 1])]),
    );
    // Auto-sized by elk from its content, not one of NODE_DIMENSIONS's fixed sizes.
    expect(group.width).toBeUndefined();
    expect(group.height).toBeUndefined();
    // Flat sibling stays a direct child of the root, not absorbed into the group.
    expect(elk.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([nodeIdOf([5, 2])]),
    );
  });

  it("keeps every edge at the root level regardless of nesting depth", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    expect(elk.edges!.map((e) => e.id)).toEqual(visible.edges.map((e) => e.id));
    for (const child of elk.children!) expect(child.edges).toBeUndefined();
  });

  it("round-trips nested positions unchanged (parent-relative, no conversion)", () => {
    const visible = deriveVisibleGraph(s, s.root);
    const grouping = groupByLir(visible.nodes);
    const elk = toElkGraph(visible, grouping);
    const group = elk.children!.find((c) => c.id === grouping.groups[0].id)!;
    group.x = 12;
    group.y = 87;
    group.width = 132;
    group.height = 259;
    const member = group.children!.find((c) => c.id === nodeIdOf([5, 1]))!;
    member.x = 16;
    member.y = 24;
    const positions = extractPositions(elk);
    // The member's extracted position is exactly its own x/y, not offset by
    // the group's: elk already reports it relative to the group.
    expect(positions[member.id]).toMatchObject({ x: 16, y: 24 });
    expect(positions[group.id]).toMatchObject({ x: 12, y: 87, width: 132, height: 259 });
  });

  it("real elk lays out nested groups with auto-sized bounds and cross-boundary edges", async () => {
    // Empirical check of the assumption the design doc relies on: elk
    // auto-sizes a compound node from its children, reports child positions
    // relative to their own parent (matching React Flow's parentId
    // convention with no coordinate conversion needed), and relocates edges
    // that cross group boundaries to the right container even when every
    // edge is declared at the root, given hierarchyHandling: INCLUDE_CHILDREN.
    const elk = new ELK();
    const graph = {
      id: "root",
      layoutOptions: {
        "elk.algorithm": "layered",
        "elk.direction": "DOWN",
        "elk.hierarchyHandling": "INCLUDE_CHILDREN",
      },
      children: [
        { id: "ungrouped", width: 100, height: 50 },
        {
          id: "groupA",
          layoutOptions: { "elk.padding": "[top=24,left=8,bottom=8,right=8]" },
          children: [
            { id: "a1", width: 100, height: 50 },
            {
              id: "groupA_inner",
              layoutOptions: { "elk.padding": "[top=24,left=8,bottom=8,right=8]" },
              children: [
                { id: "a2", width: 100, height: 50 },
                { id: "a3", width: 100, height: 50 },
              ],
            },
          ],
        },
      ],
      edges: [
        { id: "e1", sources: ["ungrouped"], targets: ["a1"] },
        { id: "e2", sources: ["a1"], targets: ["a2"] },
        { id: "e3", sources: ["a2"], targets: ["a3"] },
      ],
    };
    const result = await elk.layout(graph);
    const groupA = result.children!.find((c) => c.id === "groupA")!;
    const groupAInner = groupA.children!.find((c) => c.id === "groupA_inner")!;
    // Auto-sized larger than any single member: real content-driven sizing.
    expect(groupA.width!).toBeGreaterThan(100);
    expect(groupA.height!).toBeGreaterThan(50);
    // e3 (both endpoints inside groupA_inner) is relocated there even though
    // it was declared at the root.
    expect((result.edges ?? []).some((e) => e.id === "e3")).toBe(false);
    const findEdge = (node: typeof groupAInner, id: string) =>
      (node.edges ?? []).some((e) => e.id === id);
    expect(findEdge(groupAInner, "e3")).toBe(true);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd console && yarn vitest run src/platform/dataflows/elkGraph.test.ts`
Expected: FAIL — `toElkGraph` doesn't accept a second argument yet, and the flat-only implementation never nests anything (`group.children` is `undefined`, and the "real elk" test itself should already pass in isolation since it doesn't touch this codebase's functions — confirm it passes standalone before moving on, since it is the risk-gate check, not a test of code you're about to write).

- [ ] **Step 3: Implement nested `toElkGraph` and recursive `extractPositions`**

Replace the full contents of `console/src/platform/dataflows/elkGraph.ts` from the `LAYOUT_OPTIONS` constant to the end of the file with:

```typescript
const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.padding": "[top=48,left=16,bottom=16,right=16]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
  // Lets an edge crossing a LIR group's boundary be declared uniformly at
  // the root: elk relocates it to the correct container itself. Harmless
  // when there's no grouping at all (today's flat views).
  "elk.hierarchyHandling": "INCLUDE_CHILDREN",
};

// A LIR group is a label-only wrapper, sized purely from its content (no
// fixed width/height), with top padding reserved for the header strip
// LirGroupNode renders.
const GROUP_LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "DOWN",
  "elk.padding": "[top=24,left=8,bottom=8,right=8]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
};

// A view is always one scope's direct children, so without a grouping this
// is flat: nothing is nested inside anything else. With a grouping, a
// group's members (VisibleNodes or other groups) become its elk children;
// everything else stays a direct child of the root, exactly like today.
export function toElkGraph(graph: VisibleGraph, grouping?: LirGrouping): ElkNode {
  const nodeById = new Map(graph.nodes.map((n) => [n.id, n]));
  const groupById = new Map((grouping?.groups ?? []).map((g) => [g.id, g]));
  const childrenOf = new Map<string, string[]>(); // parent id ("" = root) -> ordered child ids
  const addChild = (parentKey: string, id: string) => {
    const list = childrenOf.get(parentKey);
    if (list) list.push(id);
    else childrenOf.set(parentKey, [id]);
  };
  for (const n of graph.nodes) addChild(grouping?.parentOf.get(n.id) ?? "", n.id);
  for (const g of groupById.values()) addChild(grouping?.parentOf.get(g.id) ?? "", g.id);

  const buildNode = (id: string): ElkNode => {
    const group = groupById.get(id);
    if (group) {
      return {
        id,
        layoutOptions: GROUP_LAYOUT_OPTIONS,
        children: (childrenOf.get(id) ?? []).map(buildNode),
      };
    }
    return { id, ...NODE_DIMENSIONS[nodeById.get(id)!.kind] };
  };

  return {
    id: "__root__",
    layoutOptions: LAYOUT_OPTIONS,
    children: (childrenOf.get("") ?? []).map(buildNode),
    edges: graph.edges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  };
}

// Recurses into nested children: elk reports every node's x/y already
// relative to its own immediate parent, the same convention React Flow uses
// for a node with parentId set, so no coordinate conversion happens here —
// this only flattens the tree into one lookup map, keyed by id at any depth.
export function extractPositions(layouted: ElkNode): Positions {
  const positions: Positions = {};
  const visit = (node: ElkNode) => {
    for (const child of node.children ?? []) {
      positions[child.id] = {
        x: child.x ?? 0,
        y: child.y ?? 0,
        width: child.width ?? 0,
        height: child.height ?? 0,
      };
      visit(child);
    }
  };
  visit(layouted);
  return positions;
}
```

Add `LirGrouping` to the existing `import type { VisibleGraph, VisibleNode } from "./dataflowGraph";` line at the top of the file (change to `import type { LirGrouping, VisibleGraph, VisibleNode } from "./dataflowGraph";`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd console && yarn vitest run src/platform/dataflows/elkGraph.test.ts`
Expected: PASS, all tests including the two pre-existing flat-graph tests (unaffected) and the four new ones.

- [ ] **Step 5: Thread `grouping` through `useElkLayout`**

In `console/src/platform/dataflows/useElkLayout.ts`:

Change the import line to:
```typescript
import type { LirGrouping, VisibleGraph } from "./dataflowGraph";
```

Change the function signature:
```typescript
export function useElkLayout(
  graph: VisibleGraph | null,
  cacheKey: string,
  grouping?: LirGrouping,
) {
```

Change the layout call inside the second effect:
```typescript
    elk.layout(toElkGraph(graph, grouping)).then(
```

Add `grouping` to that effect's dependency array:
```typescript
  }, [graph, cacheKey, retryNonce, grouping]);
```

- [ ] **Step 6: Typecheck and lint**

Run: `cd console && yarn typecheck && yarn lint`
Expected: no errors. (`useElkLayout.ts` has no dedicated test file today; its behavior is exercised through `DataflowGraphView`/`DataflowDetailPage` tests in Task 4.)

- [ ] **Step 7: Commit**

```bash
git add console/src/platform/dataflows/elkGraph.ts console/src/platform/dataflows/elkGraph.test.ts console/src/platform/dataflows/useElkLayout.ts
git commit -m "$(cat <<'EOF'
console: nest LIR groups as elk compound nodes

First use of elk's native compound-node layout in this codebase.
Verified against the real elk engine (not the worker-backed elk-api
used at runtime): groups auto-size from content, child positions come
back already relative to their own parent, and hierarchyHandling:
INCLUDE_CHILDREN relocates a boundary-crossing edge to its correct
container even when every edge is declared uniformly at the root.
EOF
)"
```

---

### Task 3: `LirGroupNode` render component and color helper

**Files:**
- Modify: `console/src/platform/dataflows/nodeStyle.ts`
- Modify: `console/src/platform/dataflows/nodes.tsx`
- Test: `console/src/platform/dataflows/nodeStyle.test.ts`

**Interfaces:**
- Consumes: nothing new from earlier tasks (this is a leaf UI unit; Task 4 wires it in).
- Produces: `lirGroupColor(lirId: string): string`, `type FlowGroupData = { group: LirGroupNode; label: string; color: string }`, `LirGroupNode` (the React Flow component, named the same as the data-layer type from Task 1 — imported under an alias wherever both are needed, see Task 4).

- [ ] **Step 1: Write the failing test**

Add to `console/src/platform/dataflows/nodeStyle.test.ts`:

```typescript
import { lirGroupColor } from "./nodeStyle";

describe("lirGroupColor", () => {
  it("is deterministic for the same lirId", () => {
    expect(lirGroupColor("42")).toEqual(lirGroupColor("42"));
  });

  it("differs for different lirIds often enough to be useful", () => {
    const colors = new Set(["1", "2", "3", "4", "5", "6"].map(lirGroupColor));
    expect(colors.size).toBeGreaterThan(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd console && yarn vitest run src/platform/dataflows/nodeStyle.test.ts`
Expected: FAIL with `lirGroupColor is not exported`.

- [ ] **Step 3: Implement `lirGroupColor` and `FlowGroupData`**

Add to `console/src/platform/dataflows/nodeStyle.ts`, after the `HIGHLIGHT_COLORS` constant:

```typescript
// A small, visually distinct palette for LIR group borders/headers, cycled
// by a deterministic hash so the same lirId always gets the same color
// within one render (and across re-renders, since lirId is stable).
const GROUP_PALETTE = [
  "#e8590c",
  "#5f3dc4",
  "#0b7285",
  "#2f9e44",
  "#c2255c",
  "#1971c2",
  "#e67700",
  "#7048e8",
];

export function lirGroupColor(lirId: string): string {
  let hash = 0;
  for (let i = 0; i < lirId.length; i++) {
    hash = (hash * 31 + lirId.charCodeAt(i)) | 0;
  }
  return GROUP_PALETTE[Math.abs(hash) % GROUP_PALETTE.length];
}
```

Add to the bottom of `console/src/platform/dataflows/nodeStyle.ts`, and add `LirGroupNode` to its existing `import type { VisibleNode } from "./dataflowGraph";` line (making it `import type { LirGroupNode, VisibleNode } from "./dataflowGraph";`):

```typescript
export type FlowGroupData = {
  group: LirGroupNode;
  label: string;
  color: string;
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd console && yarn vitest run src/platform/dataflows/nodeStyle.test.ts`
Expected: PASS.

- [ ] **Step 5: Implement the `LirGroupNode` component**

Add to `console/src/platform/dataflows/nodes.tsx`, after the existing `PortNode` component, and add `FlowGroupData` to the existing `import { ... } from "./nodeStyle";` line:

```typescript
// A label-only wrapper around its members, never itself a click target
// except its header (the body is pointerEvents="none" so clicks pass
// through to whatever member is underneath). Its width/height come from
// elk's auto-sized bounds, same as every other node in this file.
export const LirGroupNode = ({
  data,
}: NodeProps & { data: FlowGroupData }) => (
  <Box width="100%" height="100%" position="relative" pointerEvents="none">
    <Box
      position="absolute"
      inset={0}
      borderWidth="2px"
      borderStyle="dashed"
      borderRadius="md"
      borderColor={data.color}
    />
    <Box
      position="absolute"
      top={0}
      left={0}
      px={2}
      py="1px"
      pointerEvents="auto"
      cursor="pointer"
      background={data.color}
      borderBottomRightRadius="md"
    >
      <Text fontSize="2xs" color="white" noOfLines={1}>
        {data.label}
      </Text>
    </Box>
  </Box>
);
```

- [ ] **Step 6: Typecheck and lint**

Run: `cd console && yarn typecheck && yarn lint`
Expected: no errors. (No isolated component test for this one, matching `OperatorNode`/`RegionNode`/`PortNode`, none of which have dedicated test files either; Task 4's integration test covers its rendering.)

- [ ] **Step 7: Commit**

```bash
git add console/src/platform/dataflows/nodeStyle.ts console/src/platform/dataflows/nodeStyle.test.ts console/src/platform/dataflows/nodes.tsx
git commit -m "console: add LirGroupNode component and lirGroupColor helper"
```

---

### Task 4: Wire grouping into `DataflowGraphView`

**Files:**
- Modify: `console/src/platform/dataflows/DataflowGraphView.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.test.tsx` (fix the inline React Flow mock)

**Interfaces:**
- Consumes: `groupByLir`, `LirGrouping`, `LirGroupNode` (data type, Task 1); `LirGroupNode` (component, Task 3, imported as `LirGroupNodeComponent` to avoid the name clash); `lirGroupColor` (Task 3); `useElkLayout`'s new third parameter (Task 2).
- Produces: two new `DataflowGraphViewProps`: `showLirGroups?: boolean` and `onLirGroupClick?: (group: LirGroupNode) => void`, consumed by `DataflowDetailPage.tsx` in Tasks 5 and 6.

- [ ] **Step 1: Fix the test mock's node-rendering assumption**

The mock `ReactFlow` in `DataflowDetailPage.test.tsx` renders each node's label by reading `n.data.node.label`, which every operator/region/port node has but a group node (whose `data` is `{ group, label, color }`, no nested `.node`) does not. Before adding any group node to what gets rendered, fix the mock to handle both shapes.

In `console/src/platform/dataflows/DataflowDetailPage.test.tsx`, change the `vi.mock("@xyflow/react", ...)` block's `ReactFlow` implementation:

```typescript
  ReactFlow: ({
    nodes,
    children,
    onNodeClick,
    onNodeDoubleClick,
  }: {
    nodes: {
      id: string;
      type?: string;
      data: { node?: { label: string }; label?: string };
    }[];
    children?: React.ReactNode;
    onNodeClick?: (e: unknown, node: unknown) => void;
    onNodeDoubleClick?: (e: unknown, node: unknown) => void;
  }) => (
    <div data-testid="react-flow">
      {nodes.map((n) => (
        <div
          key={n.id}
          data-testid={`node-${n.id}`}
          onClick={() => onNodeClick?.(null, n)}
          onDoubleClick={() => onNodeDoubleClick?.(null, n)}
        >
          {n.data.node ? n.data.node.label : n.data.label}
        </div>
      ))}
      {children}
    </div>
  ),
```

- [ ] **Step 2: Run the full existing suite to confirm the mock fix alone changes nothing**

Run: `cd console && yarn vitest run src/platform/dataflows/DataflowDetailPage.test.tsx`
Expected: PASS, same as before this step (this is a pure widening of the mock's type, not a behavior change).

- [ ] **Step 3: Add a dedicated LIR fixture and the failing test for group rendering**

The file's existing default dataflow ("7") fixture has zero LIR spans (its mocked SQL response sends `okResult`, an empty result, for the `lirSpans` query), and every other existing test relies on that staying empty. Rather than touching it, add a new dataflow id, following the same pattern already used for dataflow 8 (`portJumpOperatorsResult`) and 9 (`fanOutOperatorsResult`).

Add these constants to `console/src/platform/dataflows/DataflowDetailPage.test.tsx`, near the other dataflow fixtures (after `fanOutChannelsResult`):

```typescript
// Dataflow 40: root [40], child [40,1] "Join" (operator id 41), covered by
// one LIR span, to exercise the "Show LIR groups" toggle in isolation from
// every other test's dataflow 7 fixture, which has no LIR spans at all.
const lirOperatorsResult = {
  ...operatorsResult,
  rows: [
    ["40", ["40"], "Dataflow", "0", "0", "0"],
    ["41", ["40", "1"], "Join", "0", "0", "0"],
  ],
};
const lirSpansResult = {
  desc: {
    columns: [
      { name: "exportId" },
      { name: "lirId" },
      { name: "parentLirId" },
      { name: "nesting" },
      { name: "operator" },
      { name: "operatorIdStart" },
      { name: "operatorIdEnd" },
    ],
  },
  rows: [["u7", "1", null, "0", "Join::Differential", "41", "42"]],
};
```

Add a new branch to the `http.post("*/api/sql", ...)` handler inside `beforeEach`, alongside the existing `'8'`/`'9'` branches (order among these branches doesn't matter, but put it before the final unconditional fallback):

```typescript
      if (body.includes("'40'")) {
        return HttpResponse.json({
          results: [lirOperatorsResult, okResult, lirSpansResult, okResult],
        });
      }
```

Then append this test to the `describe("DataflowDetailPage", ...)` block, following the same `renderComponent`/`initialRouterEntries` pattern the dataflow-8 tests already use (e.g. `"jumps to an output port's peer, same as an input port's peer"`):

```typescript
  it("shows a LIR group box when the toggle is on, and hides it when off", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/40"],
      },
    );

    expect(
      await screen.findByTestId(`node-${nodeIdOf([40, 1])}`),
    ).toBeInTheDocument();
    expect(screen.queryByText(/Join::Differential/)).not.toBeInTheDocument();

    await userEvent.click(screen.getByLabelText("Show LIR groups"));

    const groupNode = screen.getByTestId("node-u7/1");
    expect(groupNode).toHaveTextContent("Join::Differential");

    // Regression check for the onNodeDoubleClick guard added in Step 4:
    // double-clicking a group must not throw (it read `.data.node.kind`
    // unconditionally before the guard, which crashes on a group node) and
    // must not navigate anywhere, since groups aren't scopes.
    await userEvent.dblClick(groupNode);
    expect(screen.getByTestId("node-u7/1")).toBeInTheDocument();
  });
```

This references `screen.getByLabelText("Show LIR groups")`, which doesn't exist until Task 5. Run it now to confirm it fails on the missing label (not a crash), then move on — it is expected to stay red until Task 5.

- [ ] **Step 4: Implement grouping in `DataflowGraphView.tsx`**

In `console/src/platform/dataflows/DataflowGraphView.tsx`:

Change the import block to add the new pieces:
```typescript
import { ChannelEdge } from "./ChannelEdge";
import {
  type GraphDecorations,
  groupByLir,
  type LirGroupNode as LirGroupNodeData,
  type NodeId,
  type PortPeer,
  rerouteHiddenNodes,
  type VisibleEdge,
  type VisibleGraph,
  type VisibleNode,
} from "./dataflowGraph";
import { NODE_DIMENSIONS, type Positions } from "./elkGraph";
import {
  LirGroupNode as LirGroupNodeComponent,
  OperatorNode,
  PortNode,
  RegionNode,
} from "./nodes";
import { lirGroupColor, nodeFillColor } from "./nodeStyle";
import { useElkLayout } from "./useElkLayout";
```

Add `lirGroup: LirGroupNodeComponent` to `nodeTypes`:
```typescript
const nodeTypes = {
  operator: OperatorNode,
  region: RegionNode,
  port: PortNode,
  lirGroup: LirGroupNodeComponent,
};
```

Add two new props to `DataflowGraphViewProps` (near `onJumpToPeer`):
```typescript
  // Toggled from the toolbar; grouping is computed here (from the same
  // post-hideIdle, post-reroute node list elk actually lays out) rather than
  // by the caller, so a hidden node can never end up as a group's only
  // member.
  showLirGroups?: boolean;
  onLirGroupClick?: (group: LirGroupNodeData) => void;
```

Destructure them in the component signature (add `showLirGroups, onLirGroupClick,` to the existing destructured props list).

After the existing `const visible = React.useMemo(...)` block (which computes the rerouted/filtered graph), add:
```typescript
  const grouping = React.useMemo(
    () => (showLirGroups ? groupByLir(visible.nodes) : null),
    [showLirGroups, visible.nodes],
  );
```

Change `layoutKey` to include the toggle, so turning it on/off invalidates the layout cache:
```typescript
  const layoutKey = `${cacheKey}|${focusedScope}|${showLirGroups ?? false}|${
    decorations?.hiddenNodeIds
      ? [...decorations.hiddenNodeIds].sort().join(",")
      : ""
  }|${
    decorations?.hiddenEdgeIds
      ? [...decorations.hiddenEdgeIds].sort().join(",")
      : ""
  }`;
  const { positions, layouting, error, retry } = useElkLayout(
    visible,
    layoutKey,
    grouping ?? undefined,
  );
```

Replace the `nodes: Node[] = React.useMemo(...)` block with one that emits group nodes before member nodes, and sets `parentId` on anything grouping places inside a group:
```typescript
  const nodes: Node[] = React.useMemo(() => {
    if (!positions) return [];
    const groupNodes: Node[] = (grouping?.groups ?? []).map((g) => {
      const pos = positions[g.id] ?? { x: 0, y: 0, width: 0, height: 0 };
      return {
        id: g.id,
        type: "lirGroup",
        position: { x: pos.x, y: pos.y },
        parentId: grouping!.parentOf.get(g.id),
        width: pos.width,
        height: pos.height,
        // pointerEvents: "none" on React Flow's own node wrapper (not just
        // inside LirGroupNode's own JSX) is the other half of the
        // click-through contract LirGroupNode's comment documents: the
        // wrapper defaults to pointer-events:all and LirGroupNode can't
        // override an ancestor it doesn't render. With the wrapper opted
        // out, LirGroupNode's header (pointerEvents:"auto") still receives
        // clicks (a descendant can always re-enable itself), and a click on
        // the group's empty body falls through to a member's own wrapper
        // when one is there, or further to the pane when none is.
        style: { width: pos.width, height: pos.height, pointerEvents: "none" },
        draggable: false,
        connectable: false,
        // Below its members, which draw over it; React Flow requires a
        // group node to precede its children in this array, which the
        // groupNodes-then-memberNodes concat below already guarantees, but
        // an explicit zIndex also protects against member nodes elsewhere
        // in the array being reordered by a future change.
        zIndex: -1,
        data: { group: g, label: g.operator, color: lirGroupColor(g.lirId) },
      };
    });
    const memberNodes: Node[] = visible.nodes.map((n) => {
      const pos = positions[n.id] ?? { x: 0, y: 0, ...NODE_DIMENSIONS[n.kind] };
      return {
        id: n.id,
        type: n.kind,
        position: { x: pos.x, y: pos.y },
        parentId: grouping?.parentOf.get(n.id),
        // Match the Top/Bottom handles so bezier control points meet the
        // node edges. Without this the edge curves toward the default
        // Bottom/Top and appears detached across region boundaries.
        targetPosition: Position.Top,
        sourcePosition: Position.Bottom,
        // Set as explicit node fields, not just CSS style: the MiniMap and
        // culled (onlyRenderVisibleElements) off-screen nodes both need a
        // known size without waiting for DOM measurement.
        width: pos.width,
        height: pos.height,
        style: { width: pos.width, height: pos.height },
        draggable: false,
        connectable: false,
        data: {
          node: n,
          dimmed: decorations?.dimmedNodeIds?.has(n.id) ?? false,
          color: decorations?.nodeColors?.get(n.id) ?? nodeFillColor(n),
          selected: n.id === selectedId,
          activeMatch: n.id === activeMatchId,
        },
      };
    });
    return [...groupNodes, ...memberNodes];
  }, [
    visible,
    positions,
    grouping,
    decorations?.dimmedNodeIds,
    decorations?.nodeColors,
    selectedId,
    activeMatchId,
  ]);
```

Change the `onNodeClick` handler passed to `<ReactFlow>` to branch on group clicks first:
```typescript
        onNodeClick={(_, node) => {
          if (node.type === "lirGroup") {
            onLirGroupClick?.((node.data as { group: LirGroupNodeData }).group);
            return;
          }
          const visibleNode = (node.data as { node: VisibleNode }).node;
          const connectedEdges = visible.edges
            .filter(
              (e) => e.source === visibleNode.id || e.target === visibleNode.id,
            )
            .map((e) => ({
              ...e,
              sourceLabel: labelById.get(e.source) ?? e.source,
              targetLabel: labelById.get(e.target) ?? e.target,
            }));
          onNodeClick?.(visibleNode, connectedEdges);
        }}
```

Also guard the existing `onNodeDoubleClick` handler, a few lines below `onNodeClick` in the same `<ReactFlow>` element: it currently reads `(node.data as { node: VisibleNode }).node.kind` unconditionally, which throws on a group node (`data` has no `.node`). Add the same type check at the top, before that line:
```typescript
        onNodeDoubleClick={(_, node) => {
          // Groups aren't scopes: no drill-down, no jump, nothing happens.
          if (node.type === "lirGroup") return;
          const visibleNode = (node.data as { node: VisibleNode }).node;
          if (visibleNode.kind === "region") {
            onNavigate(visibleNode.id);
          } else if (
            visibleNode.kind === "port" &&
            visibleNode.peers.length === 1
          ) {
            onJumpToPeer?.(visibleNode.peers[0]);
          }
        }}
```

- [ ] **Step 5: Run typecheck to confirm wiring compiles**

Run: `cd console && yarn typecheck`
Expected: no errors. (The Task 3 test written in Step 3 is still expected to fail — it depends on Task 5's toolbar switch, which doesn't exist yet. Confirm it fails with "Unable to find a label with the text: Show LIR groups", not a crash, so you know the rest of the wiring is otherwise sound.)

Run: `cd console && yarn vitest run src/platform/dataflows/DataflowDetailPage.test.tsx`
Expected: every test PASSES except the new one from Step 3, which fails on the missing label (not a crash/type error).

- [ ] **Step 6: Lint**

Run: `cd console && yarn lint`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add console/src/platform/dataflows/DataflowGraphView.tsx console/src/platform/dataflows/DataflowDetailPage.test.tsx
git commit -m "$(cat <<'EOF'
console: wire LIR grouping into DataflowGraphView

Grouping is computed from the same post-hideIdle, post-reroute node
list elk actually lays out, so a hidden node never ends up as a
group's only member. Group nodes render behind their members via
React Flow's parentId nesting, one new nodeTypes entry, and an
onNodeClick branch for group-header clicks (wired to a real handler
in Task 6).

This task's new toggle test doesn't pass yet — it depends on Task 5's
toolbar switch, which doesn't exist.
EOF
)"
```

---

### Task 5: Toolbar toggle

**Files:**
- Modify: `console/src/platform/dataflows/dataflowGraph.ts`
- Modify: `console/src/platform/dataflows/DataflowToolbar.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Test: existing tests in `dataflowGraph.test.ts` and `DataflowDetailPage.test.tsx` (the one from Task 4, Step 3)

**Interfaces:**
- Consumes: `DataflowGraphView`'s `showLirGroups` prop (Task 4).
- Produces: `Filters.showLirGroups: boolean`, `DEFAULT_FILTERS.showLirGroups: false`.

- [ ] **Step 1: Update `DEFAULT_FILTERS` assertions if any exist, then add the field**

Search for existing assertions against `DEFAULT_FILTERS`'s exact shape:

Run: `cd console && grep -rn "DEFAULT_FILTERS" src/platform/dataflows/*.test.ts src/platform/dataflows/*.test.tsx`

If any test asserts `DEFAULT_FILTERS` equals an object literal without `showLirGroups`, update it to include `showLirGroups: false`. (As of this plan being written, no such assertion exists — `DEFAULT_FILTERS` is only referenced by identity, e.g. `React.useState<Filters>(DEFAULT_FILTERS)` — but confirm before proceeding, since the plan can't see future changes to this file.)

In `console/src/platform/dataflows/dataflowGraph.ts`, change the `Filters` interface and `DEFAULT_FILTERS` constant:

```typescript
export interface Filters {
  search: string;
  hideIdle: boolean;
  heatmap: "off" | "elapsed" | "size" | "cpuSkew" | "memorySkew";
  heatmapThreshold: number; // 0..1 fraction of max
  showLirGroups: boolean;
}

export const DEFAULT_FILTERS: Filters = {
  search: "",
  hideIdle: false,
  heatmap: "off",
  heatmapThreshold: 0,
  showLirGroups: false,
};
```

- [ ] **Step 2: Add the toolbar switch**

In `console/src/platform/dataflows/DataflowToolbar.tsx`, add a new `FormControl`/`Switch` pair after the existing "Hide idle" one:

```typescript
      <FormControl display="flex" alignItems="center" width="auto">
        <FormLabel fontSize="xs" mb={0}>
          Show LIR groups
        </FormLabel>
        <Switch
          size="sm"
          aria-label="Show LIR groups"
          isChecked={filters.showLirGroups}
          onChange={(e) =>
            onFiltersChange({ ...filters, showLirGroups: e.target.checked })
          }
        />
      </FormControl>
```

The existing "Hide idle" switch has no `aria-label`, relying on its `FormLabel` for accessible naming via Chakra's `FormControl` association; do the same here (Chakra's `FormControl` context automatically associates the `FormLabel` with the `Switch`, so `getByLabelText("Show LIR groups")` in tests resolves without an explicit `aria-label`). Remove the `aria-label` line above if Chakra's automatic association already covers it — check by running the Task 4 test after this step; add `aria-label="Show LIR groups"` back only if the test can't find it by label text otherwise.

- [ ] **Step 3: Wire the prop through `DataflowDetailPage.tsx`**

In `console/src/platform/dataflows/DataflowDetailPage.tsx`, add `showLirGroups={filters.showLirGroups}` to the `<DataflowGraphView>` element's props (alongside the existing `decorations={decorations}` line).

- [ ] **Step 4: Run the Task 4 toggle test to verify it now passes**

Run: `cd console && yarn vitest run src/platform/dataflows/DataflowDetailPage.test.tsx`
Expected: PASS, including the "shows a LIR group box when the toggle is on" test from Task 4.

- [ ] **Step 5: Run the full dataflow test suite**

Run: `cd console && yarn vitest run src/platform/dataflows src/api/materialize/dataflow`
Expected: PASS, all tests.

- [ ] **Step 6: Typecheck and lint**

Run: `cd console && yarn typecheck && yarn lint`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add console/src/platform/dataflows/dataflowGraph.ts console/src/platform/dataflows/DataflowToolbar.tsx console/src/platform/dataflows/DataflowDetailPage.tsx console/src/platform/dataflows/DataflowDetailPage.test.tsx
git commit -m "console: add Show LIR groups toolbar toggle, off by default"
```

---

### Task 6: Click a group's header to open its detail panel

**Files:**
- Modify: `console/src/platform/dataflows/LirPanel.tsx` (export the existing summary card for reuse)
- Modify: `console/src/platform/dataflows/NodeDetailPanel.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Test: `console/src/platform/dataflows/DataflowDetailPage.test.tsx`

**Interfaces:**
- Consumes: `DataflowGraphView`'s `onLirGroupClick` prop (Task 4); `lirIndex`, `lirSummary` (both already exist in `dataflowGraph.ts`, unchanged).
- Produces: `Selection` gains a `"lirGroup"` variant; no new exports beyond `LirSummaryCard`.

- [ ] **Step 1: Write the failing test**

Append to `console/src/platform/dataflows/DataflowDetailPage.test.tsx`, reusing dataflow 40's fixture added in Task 4, Step 3:

```typescript
  it("opens the detail panel with LIR summary info when a group header is clicked", async () => {
    await renderComponent(
      <Routes>
        <Route
          path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
          element={<DataflowDetailPage />}
        />
      </Routes>,
      {
        initialRouterEntries: ["/clusters/u5/test_cluster/dataflows/40"],
      },
    );

    await screen.findByTestId(`node-${nodeIdOf([40, 1])}`);
    await userEvent.click(screen.getByLabelText("Show LIR groups"));
    await userEvent.click(screen.getByTestId("node-u7/1"));

    // The title and LirSummaryCard both render "LIR 1: Join::Differential",
    // so assert at least one match rather than a single getByText, which
    // throws on more than one hit.
    expect(
      screen.getAllByText(/LIR 1: Join::Differential/).length,
    ).toBeGreaterThan(0);
    // Rows unique to LirSummaryCard, not NodeDetail's node-shaped rows.
    expect(screen.getByText("Records")).toBeInTheDocument();
    expect(screen.getByText("Memory")).toBeInTheDocument();
  });
```

(Adjust the LIR id/operator text to match whatever this file's dataflow fixture actually uses — see Task 4, Step 3's note about matching the fixture's LIR span.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd console && yarn vitest run src/platform/dataflows/DataflowDetailPage.test.tsx -t "opens the detail panel with LIR summary"`
Expected: FAIL — clicking the group header currently calls `onLirGroupClick`, which `DataflowDetailPage` doesn't pass yet, so nothing opens.

- [ ] **Step 3: Export `LirSummaryCard` for reuse**

In `console/src/platform/dataflows/LirPanel.tsx`, change:
```typescript
const LirSummaryCard = ({ node }: { node: LirTreeNode }) => (
```
to:
```typescript
export const LirSummaryCard = ({ node }: { node: LirTreeNode }) => (
```

- [ ] **Step 4: Add the `"lirGroup"` selection variant to `NodeDetailPanel.tsx`**

Change the `Selection` type:
```typescript
export type Selection =
  | { kind: "node"; node: VisibleNode; connectedEdges?: SelectedEdge[] }
  | { kind: "edge"; edge: SelectedEdge }
  | { kind: "lirGroup"; node: LirTreeNode };
```

Add `LirTreeNode` to the existing `import { ... } from "./dataflowGraph";` line, and import `LirSummaryCard` from `./LirPanel`:
```typescript
import {
  formatElapsedNs,
  type LirTreeNode,
  type PortPeer,
  type VisibleNode,
} from "./dataflowGraph";
import { LirSummaryCard } from "./LirPanel";
```

Change the `title` computation in `NodeDetailPanel`:
```typescript
  const title =
    selection.kind === "node"
      ? selection.node.label
      : selection.kind === "edge"
        ? `${selection.edge.sourceLabel} → ${selection.edge.targetLabel}`
        : `LIR ${selection.node.info.lirId}: ${selection.node.info.operator}`;
```

Change the body's conditional rendering (the `{selection.kind === "node" ? (...) : (...)}` block) to a three-way branch:
```typescript
      {selection.kind === "node" ? (
        <NodeDetail
          node={selection.node}
          connectedEdges={selection.connectedEdges}
          onJumpTo={onJumpTo}
        />
      ) : selection.kind === "edge" ? (
        <EdgeRows edge={selection.edge} />
      ) : (
        <LirSummaryCard node={selection.node} />
      )}
```

- [ ] **Step 5: Wire `onLirGroupClick` in `DataflowDetailPage.tsx`**

Add `lirIndex` and `lirSummary` to the existing `import { ... } from "./dataflowGraph";` block (they're already exported by `dataflowGraph.ts`, just not currently imported here).

Add a new callback near `onTogglePinLir`:
```typescript
  const onLirGroupClick = React.useCallback(
    (group: { id: string }) => {
      if (!data) return;
      const entry = lirIndex(data.structure).get(group.id);
      if (!entry) return;
      setSelection({
        kind: "lirGroup",
        node: {
          key: group.id,
          info: entry.info,
          memberIds: entry.memberIds,
          children: [],
          summary: lirSummary(data.structure, entry.memberIds),
        },
      });
    },
    [data],
  );
```

Add `onLirGroupClick={onLirGroupClick}` to the `<DataflowGraphView>` element's props, alongside the existing `onJumpToPeer={onJumpToPeer}` line.

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd console && yarn vitest run src/platform/dataflows/DataflowDetailPage.test.tsx`
Expected: PASS, all tests including the new one.

- [ ] **Step 7: Typecheck and lint**

Run: `cd console && yarn typecheck && yarn lint`
Expected: no errors.

- [ ] **Step 8: Full verification pass**

Run: `cd console && yarn vitest run src/platform/dataflows src/api/materialize/dataflow && yarn typecheck && yarn lint`
Expected: everything PASSES.

Manually confirm the feature end-to-end against a real `environmentd` (per `mz-run`): start it, run a query that produces nested LIR spans (e.g. a materialized view with a table function like `generate_series` nested inside another, matching the reference screenshot this plan was scoped from), open its dataflow in the visualizer, toggle "Show LIR groups" on, and confirm nested dashed boxes render without overlapping siblings, edges crossing a box boundary still draw sensibly, and clicking a box header opens the detail panel with the right LIR id/operator/summary. This is a manual step outside the automated test loop; note the outcome to the user rather than silently assuming it looks right.

- [ ] **Step 9: Commit**

```bash
git add console/src/platform/dataflows/LirPanel.tsx console/src/platform/dataflows/NodeDetailPanel.tsx console/src/platform/dataflows/DataflowDetailPage.tsx console/src/platform/dataflows/DataflowDetailPage.test.tsx
git commit -m "console: open LIR summary in the detail panel from a group header click"
```
