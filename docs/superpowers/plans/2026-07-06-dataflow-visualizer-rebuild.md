# Dataflow visualizer rebuild implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the console's graphviz-WASM dataflow visualizer with a React Flow + elkjs graph view with dataflow selection, in-place collapsible regions, filters, and stats, closing CNS-108 and CNS-109.

**Architecture:** Pure graph-model functions (`dataflowGraph.ts`) turn introspection SQL rows into a region tree, then derive a visible graph from collapse state. elkjs lays out the visible graph in a web worker. React Flow renders Chakra-styled nodes. Data hooks tag results with the params that produced them so stale data never renders.

**Tech Stack:** React 18, TypeScript, Chakra UI v2, kysely (SQL text), vitest + msw, `@xyflow/react` (new), `elkjs` (new), Vite.

**Spec:** `console/doc/design/20260706_dataflow_visualizer_rebuild.md` (approved at 1aa33ec74f). Two review items folded in: `Channel` is defined in Task 2, and `DataflowNode.lir` is an array (one entry per export span covering the operator, no disjointness assumed).

## Global constraints

* Repo: worktree `/home/moritz/dev/repos/materialize/.claude/worktrees/dataflow-viz-rebuild`, branch `moritz/dataflow-visualizer-rebuild`. All paths below relative to repo root.
* All commands run from `console/` unless stated. Test: `yarn test --run <path>`. Typecheck: `yarn typecheck`. Lint: `yarn lint`.
* Every new file starts with the BSL copyright header (copy verbatim from any existing `console/src` file).
* Commit messages: `console: <imperative summary>`. Commit after every task.
* No polling. No `d3-graphviz` or WASM. No new deps beyond `@xyflow/react` and `elkjs`.
* Catalog strings (operator names, channel types, LIR operators) must only ever render as React text nodes, never concatenated into any markup or DSL string.
* No em-dashes or structuring semicolons in comments. Comments state contracts, not narration.
* `MAX_VISIBLE_NODES = 1500` (counts nodes, not edges).
* Everything stays behind the `visualization-features` LaunchDarkly flag.
* SQL identifiers interpolated into kysely `sql` templates must go through `escapedLiteral` (`lit`); numeric ids are validated with `/^\d+$/` and cast (`::uint8`) after `lit`.

## File structure

```
console/src/platform/dataflows/
  dataflowGraph.ts          types + buildDataflowStructure + deriveVisibleGraph + decorateGraph (pure)
  dataflowGraph.test.ts
  elkGraph.ts               VisibleGraph -> ELK JSON, ELK result -> positions (pure)
  elkGraph.test.ts
  layout.worker.ts          module worker: elk.bundled.js + LayoutRequest/LayoutResponse
  useElkLayout.ts           hook owning the worker, request ids, position cache
  nodes.tsx                 OperatorNode, RegionNode, CollapsedRegionNode, PortNode + dimensions
  ChannelEdge.tsx           custom edge with label + channel-type tooltip
  DataflowGraphView.tsx     React Flow canvas wiring
  DataflowsPage.tsx         cluster-level list page + export deep-link resolution
  DataflowDetailPage.tsx    graph page: selectors, toolbar, panels, error states
  DataflowToolbar.tsx       search / hide-idle / heatmap / channel-type controls
  NodeDetailPanel.tsx       click-a-node side panel
  LirPanel.tsx              LIR operator list + hover highlight
  DataflowDetailPage.test.tsx
console/src/api/materialize/dataflow/
  useDataflowGraphData.ts   structure queries keyed by dataflow id, params-tagged results
  useDataflowList.ts        selector list query
  useDataflowIdForExport.ts export id -> dataflow id
console/src/test/mockReactFlow.tsx   shared @xyflow/react mock for jsdom tests
```

Modified: `console/package.json`, `console/src/platform/clusters/ClusterDetail.tsx`, `console/src/platform/object-explorer/SimpleObjectDetailRoutes.tsx`, spec doc.
Deleted at the end: `console/src/platform/clusters/DataflowVisualizer.tsx`, `console/src/api/materialize/useDataflowStructure.ts`, `d3-graphviz` dep.

---

## Phase 1: gated vertical slice

### Task 1: Dependencies and spec amendment

**Files:**
- Modify: `console/package.json`
- Modify: `console/doc/design/20260706_dataflow_visualizer_rebuild.md`

**Interfaces:**
- Produces: `@xyflow/react` and `elkjs` installable, importable.

- [ ] **Step 1: Add deps**

Run: `cd console && yarn add @xyflow/react elkjs`
Expected: `package.json` gains both under `dependencies`, `yarn.lock` updated. No other dep changes (`git diff yarn.lock | grep '^+version' | wc -l` small).

- [ ] **Step 2: Amend spec worker mechanism**

The spec pinned `elkjs/lib/elk-worker.min.js?worker`. We use the strictly more Vite-native mechanism instead: our own module worker (`layout.worker.ts`) that imports `elkjs/lib/elk.bundled.js` and speaks the spec's `LayoutRequest`/`LayoutResponse` protocol directly. Same code off the main thread, first-class Vite dev + build support, and the protocol wrapper is needed anyway for stale-response dropping. In the spec's rendering section, replace the two sentences pinning the mechanism with:

```
The console builds with Vite, so layout runs in a Vite module worker (`new Worker(new URL("./layout.worker.ts", import.meta.url), { type: "module" })`) that imports `elkjs/lib/elk.bundled.js` and implements the layout request protocol below.
Phase 1 verifies this in both `vite dev` and the production build.
```

Also replace the spec's fallback sentence (it references the prebuilt worker script that no longer exists under this mechanism) with:

```
If the module worker fails to bundle or run `elk.bundled.js`, the fallback is running elkjs on the main thread in a lazily loaded chunk, accepting UI stalls during layout.
```

- [ ] **Step 3: Typecheck + commit**

Run: `yarn typecheck`
Expected: PASS (no source changes yet).

```bash
git add console/package.json console/yarn.lock console/doc/design/20260706_dataflow_visualizer_rebuild.md
git commit -m "console: add @xyflow/react and elkjs for dataflow visualizer rebuild"
```

### Task 2: Graph model types and buildDataflowStructure

**Files:**
- Create: `console/src/platform/dataflows/dataflowGraph.ts`
- Test: `console/src/platform/dataflows/dataflowGraph.test.ts`

**Interfaces:**
- Produces (exact, later tasks depend on these):

```typescript
export type Address = number[];
export type NodeId = string;
export function nodeIdOf(address: Address): NodeId; // JSON.stringify(address)

export interface NodeStats {
  arrangementRecords: bigint;
  arrangementSize: bigint;
  elapsedNs: bigint;
}

export interface LirInfo { exportId: string; lirId: string; operator: string; }

export interface DataflowNode {
  id: NodeId;
  address: Address;
  name: string;
  parent: NodeId | null;
  children: NodeId[];
  own: NodeStats;
  transitive: NodeStats; // own + subtree, precomputed
  lir: LirInfo[]; // one entry per export span covering this operator id
}

export interface Channel {
  id: number;
  fromAddress: Address;
  fromPort: number;
  toAddress: Address;
  toPort: number;
  messagesSent: bigint;
  batchesSent: bigint;
  channelType: string | null;
}

export interface DataflowStructure {
  nodes: Map<NodeId, DataflowNode>;
  root: NodeId;
  channels: Channel[];
}

// SQL row shapes (values arrive as strings or numbers, normalized here)
export interface OperatorRow {
  id: bigint | number | string;
  address: string[];
  name: string;
  arrangementRecords: bigint | number | string | null;
  arrangementSize: bigint | number | string | null;
  elapsedNs: bigint | number | string | null;
}
export interface ChannelRow {
  id: number | string;
  fromOperatorAddress: string[];
  fromPort: number | string;
  toOperatorAddress: string[];
  toPort: number | string;
  messagesSent: bigint | number | string;
  batchesSent: bigint | number | string;
  channelType: string | null;
}
export interface LirSpanRow {
  exportId: string;
  lirId: string;
  operator: string;
  operatorIdStart: bigint | number | string;
  operatorIdEnd: bigint | number | string;
}

export function buildDataflowStructure(
  operators: OperatorRow[],
  channels: ChannelRow[],
  lirSpans: LirSpanRow[],
): DataflowStructure; // throws if not exactly one root (address length 1)
```

- [ ] **Step 1: Write the failing tests**

`dataflowGraph.test.ts` (header + imports elided in later snippets, always include them):

```typescript
import { describe, expect, it } from "vitest";

import {
  buildDataflowStructure,
  nodeIdOf,
  type ChannelRow,
  type LirSpanRow,
  type OperatorRow,
} from "./dataflowGraph";

// Dataflow 5: root [5], region [5,1] with children [5,1,1], [5,1,2], leaf [5,2].
export const OPS: OperatorRow[] = [
  { id: "10", address: ["5"], name: "Dataflow", arrangementRecords: null, arrangementSize: null, elapsedNs: "0" },
  { id: "11", address: ["5", "1"], name: "Region", arrangementRecords: "0", arrangementSize: "0", elapsedNs: "5" },
  { id: "12", address: ["5", "1", "1"], name: "Join", arrangementRecords: "100", arrangementSize: "4096", elapsedNs: "7" },
  { id: "13", address: ["5", "1", "2"], name: "Map", arrangementRecords: "0", arrangementSize: "0", elapsedNs: "1" },
  { id: "14", address: ["5", "2"], name: "Sink", arrangementRecords: "0", arrangementSize: "0", elapsedNs: "2" },
];
export const CHANNELS: ChannelRow[] = [
  // into the region: leaf [5,2] <- region output; region input port -> child
  { id: "1", fromOperatorAddress: ["5", "1", "0"], fromPort: "0", toOperatorAddress: ["5", "1", "1"], toPort: "0", messagesSent: "3", batchesSent: "1", channelType: "rows" },
  { id: "2", fromOperatorAddress: ["5", "1", "1"], fromPort: "0", toOperatorAddress: ["5", "1", "2"], toPort: "0", messagesSent: "5", batchesSent: "2", channelType: "rows" },
  { id: "3", fromOperatorAddress: ["5", "1"], fromPort: "0", toOperatorAddress: ["5", "2"], toPort: "0", messagesSent: "5", batchesSent: "2", channelType: "batches" },
];
export const LIR_SPANS: LirSpanRow[] = [
  { exportId: "u42", lirId: "1", operator: "Join::Differential", operatorIdStart: "11", operatorIdEnd: "13" },
];

describe("buildDataflowStructure", () => {
  it("builds the region tree from address prefixes", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.root).toEqual(nodeIdOf([5]));
    const root = s.nodes.get(s.root)!;
    expect(root.children).toEqual([nodeIdOf([5, 1]), nodeIdOf([5, 2])]);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.parent).toEqual(s.root);
    expect(region.children).toEqual([nodeIdOf([5, 1, 1]), nodeIdOf([5, 1, 2])]);
  });

  it("precomputes transitive stats", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.own.elapsedNs).toEqual(5n);
    expect(region.transitive.elapsedNs).toEqual(13n);
    expect(region.transitive.arrangementRecords).toEqual(100n);
    expect(region.transitive.arrangementSize).toEqual(4096n);
  });

  it("maps LIR spans to operators by id range, as an array", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.nodes.get(nodeIdOf([5, 1, 1]))!.lir).toEqual([
      { exportId: "u42", lirId: "1", operator: "Join::Differential" },
    ]);
    // end is exclusive
    expect(s.nodes.get(nodeIdOf([5, 2]))!.lir).toEqual([]);
  });

  it("throws without exactly one root", () => {
    expect(() => buildDataflowStructure([], [], [])).toThrow();
  });

  it("normalizes channels", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, []);
    expect(s.channels[0]).toEqual({
      id: 1, fromAddress: [5, 1, 0], fromPort: 0, toAddress: [5, 1, 1], toPort: 0,
      messagesSent: 3n, batchesSent: 1n, channelType: "rows",
    });
  });
});
```

- [ ] **Step 2: Run tests, verify failure**

Run: `yarn test --run src/platform/dataflows/dataflowGraph.test.ts`
Expected: FAIL, cannot resolve `./dataflowGraph`.

- [ ] **Step 3: Implement**

```typescript
export function nodeIdOf(address: Address): NodeId {
  return JSON.stringify(address);
}

const toBigInt = (v: bigint | number | string | null | undefined): bigint =>
  v == null ? 0n : BigInt(v);

export function buildDataflowStructure(
  operators: OperatorRow[],
  channels: ChannelRow[],
  lirSpans: LirSpanRow[],
): DataflowStructure {
  const spans = lirSpans.map((s) => ({
    exportId: s.exportId,
    lirId: s.lirId,
    operator: s.operator,
    start: toBigInt(s.operatorIdStart),
    end: toBigInt(s.operatorIdEnd),
  }));
  const nodes = new Map<NodeId, DataflowNode>();
  for (const row of operators) {
    const address = row.address.map(Number);
    const id = nodeIdOf(address);
    const opId = toBigInt(row.id);
    const own: NodeStats = {
      arrangementRecords: toBigInt(row.arrangementRecords),
      arrangementSize: toBigInt(row.arrangementSize),
      elapsedNs: toBigInt(row.elapsedNs),
    };
    nodes.set(id, {
      id,
      address,
      name: row.name,
      parent: address.length > 1 ? nodeIdOf(address.slice(0, -1)) : null,
      children: [],
      own,
      transitive: own, // replaced below
      lir: spans
        .filter((s) => s.start <= opId && opId < s.end)
        .map(({ exportId, lirId, operator }) => ({ exportId, lirId, operator })),
    });
  }
  const roots: NodeId[] = [];
  for (const node of nodes.values()) {
    if (node.parent === null) roots.push(node.id);
    else nodes.get(node.parent)?.children.push(node.id);
  }
  if (roots.length !== 1) {
    throw new Error(`expected exactly one root operator, found ${roots.length}`);
  }
  // Children in address order so layout and tests are deterministic.
  for (const node of nodes.values()) {
    node.children.sort((a, b) => {
      const [x, y] = [nodes.get(a)!.address, nodes.get(b)!.address];
      return x[x.length - 1] - y[y.length - 1];
    });
  }
  const fillTransitive = (id: NodeId): NodeStats => {
    const node = nodes.get(id)!;
    const t = { ...node.own };
    for (const c of node.children) {
      const ct = fillTransitive(c);
      t.arrangementRecords += ct.arrangementRecords;
      t.arrangementSize += ct.arrangementSize;
      t.elapsedNs += ct.elapsedNs;
    }
    node.transitive = t;
    return t;
  };
  fillTransitive(roots[0]);
  return {
    nodes,
    root: roots[0],
    channels: channels.map((c) => ({
      id: Number(c.id),
      fromAddress: c.fromOperatorAddress.map(Number),
      fromPort: Number(c.fromPort),
      toAddress: c.toOperatorAddress.map(Number),
      toPort: Number(c.toPort),
      messagesSent: toBigInt(c.messagesSent),
      batchesSent: toBigInt(c.batchesSent),
      channelType: c.channelType,
    })),
  };
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `yarn test --run src/platform/dataflows/dataflowGraph.test.ts`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add console/src/platform/dataflows/
git commit -m "console: add dataflow graph model and structure builder"
```

### Task 3: deriveVisibleGraph (collapse, ports, edge aggregation)

**Files:**
- Modify: `console/src/platform/dataflows/dataflowGraph.ts`
- Test: `console/src/platform/dataflows/dataflowGraph.test.ts`

**Interfaces:**
- Produces:

```typescript
export type CollapseState = ReadonlySet<NodeId>; // collapsed region ids

export interface VisibleNode {
  id: string; // NodeId, or `${scopeId}:in:${port}` / `${scopeId}:out:${port}` for ports
  kind: "operator" | "region" | "collapsedRegion" | "port";
  label: string;
  parent: string | null; // enclosing visible region, null when direct child of root
  stats: NodeStats | null; // own for operator/region, transitive for collapsedRegion, null for port
  transitive: NodeStats | null; // null for port
  childCount: number;
  lir: LirInfo[];
  address: Address | null; // null for ports
}

export interface VisibleEdge {
  id: string; // `${source}=>${target}`
  source: string;
  target: string;
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[]; // sorted set union of aggregated channels
}

export interface VisibleGraph { nodes: VisibleNode[]; edges: VisibleEdge[]; }

// Nodes are emitted parents-before-children (React Flow requirement).
// The root is the canvas itself, never a node. Collapsed descendants of a
// collapsed region are absorbed into it. Edges between endpoints that map to
// the same visible node are dropped.
export function deriveVisibleGraph(
  structure: DataflowStructure,
  collapsed: CollapseState,
): VisibleGraph;

export function defaultCollapseState(structure: DataflowStructure): CollapseState;
// every region except the root collapsed

export function visibleNodeCount(structure: DataflowStructure, collapsed: CollapseState): number;
export const MAX_VISIBLE_NODES = 1500;
```

- [ ] **Step 1: Write the failing tests**

Append to `dataflowGraph.test.ts`:

```typescript
import {
  defaultCollapseState,
  deriveVisibleGraph,
  MAX_VISIBLE_NODES,
  visibleNodeCount,
} from "./dataflowGraph";

describe("deriveVisibleGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const regionId = nodeIdOf([5, 1]);

  it("collapses regions to a single node with remapped edges", () => {
    const g = deriveVisibleGraph(s, new Set([regionId]));
    expect(g.nodes.map((n) => [n.id, n.kind])).toEqual([
      [regionId, "collapsedRegion"],
      [nodeIdOf([5, 2]), "operator"],
    ]);
    // channel 3 region -> sink survives, channels 1 and 2 are internal
    expect(g.edges).toEqual([
      {
        id: `${regionId}=>${nodeIdOf([5, 2])}`,
        source: regionId,
        target: nodeIdOf([5, 2]),
        messagesSent: 5n,
        batchesSent: 2n,
        channelTypes: ["batches"],
      },
    ]);
    const collapsed = g.nodes[0];
    expect(collapsed.stats).toEqual(s.nodes.get(regionId)!.transitive);
    expect(collapsed.childCount).toEqual(2);
  });

  it("expands regions with port pseudo-nodes, parents before children", () => {
    const g = deriveVisibleGraph(s, new Set());
    const ids = g.nodes.map((n) => n.id);
    expect(ids.indexOf(regionId)).toBeLessThan(ids.indexOf(nodeIdOf([5, 1, 1])));
    const port = g.nodes.find((n) => n.kind === "port")!;
    expect(port.id).toEqual(`${regionId}:in:0`);
    expect(port.parent).toEqual(regionId);
    expect(port.label).toEqual("input 0");
    // port -> Join edge preserved
    expect(g.edges.map((e) => e.id)).toContain(`${regionId}:in:0=>${nodeIdOf([5, 1, 1])}`);
  });

  it("aggregates parallel channels between the same visible pair", () => {
    const extra = buildDataflowStructure(OPS, [
      ...CHANNELS,
      { id: "4", fromOperatorAddress: ["5", "1"], fromPort: "1", toOperatorAddress: ["5", "2"], toPort: "1", messagesSent: "7", batchesSent: "1", channelType: "rows" },
    ], []);
    const g = deriveVisibleGraph(extra, new Set([regionId]));
    const e = g.edges.find((e) => e.target === nodeIdOf([5, 2]))!;
    expect(e.messagesSent).toEqual(12n);
    expect(e.channelTypes).toEqual(["batches", "rows"]);
  });

  it("defaultCollapseState collapses all non-root regions", () => {
    expect(defaultCollapseState(s)).toEqual(new Set([regionId]));
    expect(visibleNodeCount(s, defaultCollapseState(s))).toEqual(2);
    expect(MAX_VISIBLE_NODES).toEqual(1500);
  });
});
```

- [ ] **Step 2: Run tests, verify failure**

Run: `yarn test --run src/platform/dataflows/dataflowGraph.test.ts`
Expected: FAIL, `deriveVisibleGraph` not exported.

- [ ] **Step 3: Implement**

```typescript
export const MAX_VISIBLE_NODES = 1500;

// Shallowest collapsed ancestor absorbs everything beneath it.
function representative(address: Address, collapsed: CollapseState): NodeId {
  for (let len = 1; len < address.length; len++) {
    const prefix = nodeIdOf(address.slice(0, len));
    if (collapsed.has(prefix)) return prefix;
  }
  return nodeIdOf(address);
}

// A channel endpoint address ending in 0 denotes the enclosing scope's
// input (from side) or output (to side) port.
function endpointId(
  address: Address,
  port: number,
  side: "from" | "to",
  collapsed: CollapseState,
): { id: string; port?: { scope: NodeId; direction: "input" | "output"; port: number } } {
  if (address[address.length - 1] === 0) {
    const scope = address.slice(0, -1);
    const scopeId = nodeIdOf(scope);
    const rep = scope.length === 0 ? scopeId : representative(scope, collapsed);
    if (rep !== scopeId || collapsed.has(scopeId)) return { id: rep };
    const direction = side === "from" ? "input" : "output";
    return {
      id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
      port: { scope: scopeId, direction, port },
    };
  }
  return { id: representative(address, collapsed) };
}

export function deriveVisibleGraph(
  structure: DataflowStructure,
  collapsed: CollapseState,
): VisibleGraph {
  const nodes: VisibleNode[] = [];
  const emit = (id: NodeId, parent: string | null) => {
    const node = structure.nodes.get(id)!;
    const isCollapsed = collapsed.has(id) && node.children.length > 0;
    const kind =
      node.children.length === 0 ? "operator" : isCollapsed ? "collapsedRegion" : "region";
    nodes.push({
      id,
      kind,
      label: node.name,
      parent,
      stats: kind === "collapsedRegion" ? node.transitive : node.own,
      transitive: node.transitive,
      childCount: node.children.length,
      lir: node.lir,
      address: node.address,
    });
    if (!isCollapsed) for (const c of node.children) emit(c, id);
  };
  for (const c of structure.nodes.get(structure.root)!.children) emit(c, null);

  const edgesById = new Map<string, VisibleEdge & { typeSet: Set<string> }>();
  const ports = new Map<string, VisibleNode>();
  for (const ch of structure.channels) {
    const from = endpointId(ch.fromAddress, ch.fromPort, "from", collapsed);
    const to = endpointId(ch.toAddress, ch.toPort, "to", collapsed);
    if (from.id === to.id) continue;
    for (const p of [from.port, to.port]) {
      if (p && !ports.has(`${p.scope}:${p.direction === "input" ? "in" : "out"}:${p.port}`)) {
        const id = `${p.scope}:${p.direction === "input" ? "in" : "out"}:${p.port}`;
        ports.set(id, {
          id,
          kind: "port",
          label: `${p.direction} ${p.port}`,
          parent: p.scope === structure.root ? null : p.scope,
          stats: null,
          transitive: null,
          childCount: 0,
          lir: [],
          address: null,
        });
      }
    }
    const id = `${from.id}=>${to.id}`;
    const existing = edgesById.get(id);
    if (existing) {
      existing.messagesSent += ch.messagesSent;
      existing.batchesSent += ch.batchesSent;
      if (ch.channelType) existing.typeSet.add(ch.channelType);
    } else {
      edgesById.set(id, {
        id,
        source: from.id,
        target: to.id,
        messagesSent: ch.messagesSent,
        batchesSent: ch.batchesSent,
        channelTypes: [],
        typeSet: new Set(ch.channelType ? [ch.channelType] : []),
      });
    }
  }
  const edges = [...edgesById.values()].map(({ typeSet, ...e }) => ({
    ...e,
    channelTypes: [...typeSet].sort(),
  }));
  return { nodes: [...nodes, ...ports.values()], edges };
}

export function defaultCollapseState(structure: DataflowStructure): CollapseState {
  const collapsed = new Set<NodeId>();
  for (const node of structure.nodes.values()) {
    if (node.children.length > 0 && node.id !== structure.root) collapsed.add(node.id);
  }
  return collapsed;
}

export function visibleNodeCount(
  structure: DataflowStructure,
  collapsed: CollapseState,
): number {
  return deriveVisibleGraph(structure, collapsed).nodes.length;
}
```

Note: edge endpoints can reference a node id that is not visible when a channel endpoint lies inside an expanded region that the other endpoint's collapsed ancestor absorbed. `representative` guarantees both endpoints are visible ids by construction (any collapsed prefix wins). Port nodes are added to the node list after regions, and their `parent` region always precedes them because region nodes were emitted first.

- [ ] **Step 4: Run tests, verify pass**

Run: `yarn test --run src/platform/dataflows/dataflowGraph.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add console/src/platform/dataflows/
git commit -m "console: derive visible dataflow graph from collapse state"
```

### Task 4: ELK graph conversion and layout worker

**Files:**
- Create: `console/src/platform/dataflows/elkGraph.ts`
- Create: `console/src/platform/dataflows/layout.worker.ts`
- Create: `console/src/platform/dataflows/useElkLayout.ts`
- Test: `console/src/platform/dataflows/elkGraph.test.ts`

**Interfaces:**
- Consumes: `VisibleGraph`, `VisibleNode` from Task 3.
- Produces:

```typescript
// elkGraph.ts
export interface NodePosition { x: number; y: number; width: number; height: number; }
export type Positions = Record<string, NodePosition>; // key: visible node id, x/y relative to parent
export const NODE_DIMENSIONS: Record<VisibleNode["kind"], { width: number; height: number }>;
export function toElkGraph(graph: VisibleGraph): ElkNode; // nested by parent
export function extractPositions(layouted: ElkNode): Positions;

// layout.worker.ts protocol
export interface LayoutRequest { requestId: number; graph: VisibleGraph; }
export interface LayoutResponse { requestId: number; positions?: Positions; error?: string; }

// useElkLayout.ts
export function useElkLayout(
  graph: VisibleGraph | null,
  cacheKey: string, // caller-provided, e.g. structure key + sorted collapsed ids
): { positions: Positions | null; layouting: boolean; error: string | null };
```

- [ ] **Step 1: Write the failing tests**

`elkGraph.test.ts`:

```typescript
import { describe, expect, it } from "vitest";

import { buildDataflowStructure, deriveVisibleGraph, nodeIdOf } from "./dataflowGraph";
import { CHANNELS, LIR_SPANS, OPS } from "./dataflowGraph.test";
import { extractPositions, NODE_DIMENSIONS, toElkGraph } from "./elkGraph";

describe("toElkGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);

  it("nests elk children by visible parent", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, new Set()));
    const region = elk.children!.find((c) => c.id === nodeIdOf([5, 1]))!;
    expect(region.children!.map((c) => c.id)).toEqual(
      expect.arrayContaining([
        nodeIdOf([5, 1, 1]),
        nodeIdOf([5, 1, 2]),
        `${nodeIdOf([5, 1])}:in:0`,
      ]),
    );
    const leaf = region.children!.find((c) => c.id === nodeIdOf([5, 1, 1]))!;
    expect(leaf.width).toEqual(NODE_DIMENSIONS.operator.width);
  });

  it("round-trips positions relative to parent", () => {
    const elk = toElkGraph(deriveVisibleGraph(s, new Set()));
    // simulate a layout result
    elk.children![0].x = 10;
    elk.children![0].y = 20;
    const positions = extractPositions(elk);
    expect(positions[elk.children![0].id]).toMatchObject({ x: 10, y: 20 });
  });
});
```

Export `OPS`, `CHANNELS`, `LIR_SPANS` from `dataflowGraph.test.ts` (they already are, per Task 2).

- [ ] **Step 2: Run tests, verify failure**

Run: `yarn test --run src/platform/dataflows/elkGraph.test.ts`
Expected: FAIL, module missing.

- [ ] **Step 3: Implement elkGraph.ts**

```typescript
import type { ElkNode } from "elkjs/lib/elk-api";

import type { VisibleGraph, VisibleNode } from "./dataflowGraph";

export const NODE_DIMENSIONS: Record<VisibleNode["kind"], { width: number; height: number }> = {
  operator: { width: 240, height: 72 },
  collapsedRegion: { width: 260, height: 88 },
  region: { width: 0, height: 0 }, // sized by elk from children
  port: { width: 90, height: 24 },
};

const LAYOUT_OPTIONS: Record<string, string> = {
  "elk.algorithm": "layered",
  "elk.direction": "RIGHT",
  "elk.hierarchyHandling": "INCLUDE_CHILDREN",
  "elk.padding": "[top=48,left=16,bottom=16,right=16]",
  "elk.spacing.nodeNode": "24",
  "elk.layered.spacing.nodeNodeBetweenLayers": "48",
};

export function toElkGraph(graph: VisibleGraph): ElkNode {
  const elkNodes = new Map<string, ElkNode>();
  const root: ElkNode = { id: "__root__", layoutOptions: LAYOUT_OPTIONS, children: [], edges: [] };
  for (const node of graph.nodes) {
    const dims = NODE_DIMENSIONS[node.kind];
    const elkNode: ElkNode = {
      id: node.id,
      children: [],
      ...(node.kind === "region" ? { layoutOptions: LAYOUT_OPTIONS } : dims),
    };
    elkNodes.set(node.id, elkNode);
    const parent = node.parent ? elkNodes.get(node.parent)! : root;
    parent.children!.push(elkNode);
  }
  root.edges = graph.edges.map((e) => ({
    id: e.id,
    sources: [e.source],
    targets: [e.target],
  }));
  return root;
}

export function extractPositions(layouted: ElkNode): Positions {
  const positions: Positions = {};
  const walk = (node: ElkNode) => {
    for (const child of node.children ?? []) {
      positions[child.id] = {
        x: child.x ?? 0,
        y: child.y ?? 0,
        width: child.width ?? 0,
        height: child.height ?? 0,
      };
      walk(child);
    }
  };
  walk(layouted);
  return positions;
}
```

Note: `graph.nodes` is parents-before-children except ports, which come last but whose parent regions exist by then, so the single pass with `elkNodes.get(node.parent)` is safe. Hierarchical edges at the root level with `INCLUDE_CHILDREN` is the documented elk pattern for cross-hierarchy edges.

- [ ] **Step 4: Run tests, verify pass**

Run: `yarn test --run src/platform/dataflows/elkGraph.test.ts`
Expected: PASS.

- [ ] **Step 5: Implement worker + hook (no unit test, covered by phase 1 gate and stubbed in component tests)**

`layout.worker.ts`:

```typescript
import ELK from "elkjs/lib/elk.bundled.js";

import type { VisibleGraph } from "./dataflowGraph";
import { extractPositions, toElkGraph, type Positions } from "./elkGraph";

export interface LayoutRequest { requestId: number; graph: VisibleGraph; }
export interface LayoutResponse { requestId: number; positions?: Positions; error?: string; }

const elk = new ELK();

self.onmessage = async (event: MessageEvent<LayoutRequest>) => {
  const { requestId, graph } = event.data;
  try {
    const layouted = await elk.layout(toElkGraph(graph));
    const response: LayoutResponse = { requestId, positions: extractPositions(layouted) };
    self.postMessage(response);
  } catch (error) {
    self.postMessage({ requestId, error: String(error) } satisfies LayoutResponse);
  }
};
```

`useElkLayout.ts`:

```typescript
import React from "react";

import type { VisibleGraph } from "./dataflowGraph";
import type { LayoutRequest, LayoutResponse } from "./layout.worker";
import type { Positions } from "./elkGraph";

export function useElkLayout(graph: VisibleGraph | null, cacheKey: string) {
  const workerRef = React.useRef<Worker | null>(null);
  const requestIdRef = React.useRef(0);
  const cacheRef = React.useRef(new Map<string, Positions>());
  const [state, setState] = React.useState<{
    key: string | null;
    positions: Positions | null;
    error: string | null;
  }>({ key: null, positions: null, error: null });

  React.useEffect(() => {
    const worker = new Worker(new URL("./layout.worker.ts", import.meta.url), {
      type: "module",
    });
    workerRef.current = worker;
    return () => worker.terminate();
  }, []);

  React.useEffect(() => {
    if (!graph) return;
    const cached = cacheRef.current.get(cacheKey);
    if (cached) {
      setState({ key: cacheKey, positions: cached, error: null });
      return;
    }
    const requestId = ++requestIdRef.current;
    const worker = workerRef.current;
    if (!worker) return;
    const onMessage = (event: MessageEvent<LayoutResponse>) => {
      // Drop responses for superseded requests.
      if (event.data.requestId !== requestIdRef.current) return;
      if (event.data.positions) {
        cacheRef.current.set(cacheKey, event.data.positions);
        setState({ key: cacheKey, positions: event.data.positions, error: null });
      } else {
        setState({ key: cacheKey, positions: null, error: event.data.error ?? "layout failed" });
      }
    };
    worker.addEventListener("message", onMessage);
    worker.postMessage({ requestId, graph } satisfies LayoutRequest);
    return () => worker.removeEventListener("message", onMessage);
  }, [graph, cacheKey]);

  return {
    positions: state.key === cacheKey ? state.positions : null,
    layouting: graph !== null && state.key !== cacheKey,
    error: state.key === cacheKey ? state.error : null,
  };
}
```

- [ ] **Step 6: Typecheck + commit**

Run: `yarn typecheck`
Expected: PASS. If `elkjs/lib/elk.bundled.js` lacks types, add `console/src/types/elkjs-bundled.d.ts` with `declare module "elkjs/lib/elk.bundled.js" { export { default } from "elkjs/lib/elk-api"; }` and include it in the commit.

```bash
git add console/src/platform/dataflows/ console/src/types/ 2>/dev/null || git add console/src/platform/dataflows/
git commit -m "console: elk layout worker and hook for dataflow graphs"
```

### Task 5: Data hook keyed by dataflow id with params tagging

**Files:**
- Create: `console/src/api/materialize/dataflow/useDataflowGraphData.ts`
- Test: `console/src/api/materialize/dataflow/useDataflowGraphData.test.ts`

**Interfaces:**
- Consumes: `buildDataflowStructure`, `DataflowStructure` (Task 2), `useSqlManyTyped`, `escapedLiteral as lit`, `queryBuilder` (existing, see `console/src/api/materialize/useDataflowStructure.ts` for import shapes).
- Produces:

```typescript
export interface DataflowGraphParams {
  clusterName: string;
  replicaName: string;
  dataflowId: string; // numeric string, mz_dataflows.id on the selected replica
}
export function useDataflowGraphData(params?: DataflowGraphParams): {
  data: { structure: DataflowStructure; fetchedAt: Date } | null;
  error: unknown;
  databaseError: { code?: string } | null; // same shape useDataflowStructure exposes today
  loading: boolean;
  refetch: () => void;
};
```

Contract: `data` is non-null only when the last successful result was produced by exactly the current `params` and there is no error. Error always wins (CNS-109). A same-params `refetch` keeps `data` while `loading` is true so the graph refreshes in place.

- [ ] **Step 1: Write the failing test**

Use msw like the CNS-109 issue test. `useDataflowGraphData.test.ts`, using `renderHook` from `@testing-library/react` and the msw `server` from `~/api/mocks/server`:

```typescript
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";

import { ErrorCode } from "~/api/materialize/types";
import server from "~/api/mocks/server";

import { useDataflowGraphData } from "./useDataflowGraphData";

const FAILING_REPLICA = "r2";
const okResult = {
  desc: { columns: [] },
  rows: [],
};
// operators result with one root row so buildDataflowStructure succeeds
const operatorsResult = {
  desc: {
    columns: [
      { name: "id" }, { name: "address" }, { name: "name" },
      { name: "arrangementRecords" }, { name: "arrangementSize" }, { name: "elapsedNs" },
    ],
  },
  rows: [["10", ["7"], "Dataflow", "0", "0", "0"]],
};

beforeEach(() => {
  server.use(
    http.post("*/api/sql", async ({ request }) => {
      const options = JSON.parse(
        new URL(request.url).searchParams.get("options") ?? "{}",
      );
      if (options.cluster_replica === FAILING_REPLICA) {
        return HttpResponse.json({
          results: [
            { error: { message: "no such replica", code: ErrorCode.INTERNAL_ERROR } },
          ],
        });
      }
      return HttpResponse.json({ results: [operatorsResult, okResult, okResult] });
    }),
  );
});

describe("useDataflowGraphData", () => {
  it("clears data when a refetch for new params fails", async () => {
    const { result, rerender } = renderHook(
      (params) => useDataflowGraphData(params),
      {
        initialProps: { clusterName: "c", replicaName: "r1", dataflowId: "7" } as
          | { clusterName: string; replicaName: string; dataflowId: string }
          | undefined,
      },
    );
    await waitFor(() => expect(result.current.data).not.toBeNull());

    rerender({ clusterName: "c", replicaName: FAILING_REPLICA, dataflowId: "7" });
    await waitFor(() => expect(result.current.error).toBeTruthy());
    // The retained previous result must not surface for the new params.
    expect(result.current.data).toBeNull();
  });

  it("rejects a non-numeric dataflow id", () => {
    expect(() =>
      renderHook(() =>
        useDataflowGraphData({ clusterName: "c", replicaName: "r1", dataflowId: "7; DROP" }),
      ),
    ).toThrow();
  });
});
```

`mapRowToObject` (`console/src/api/materialize/index.ts`) maps column aliases to object keys verbatim, so these mocked shapes match the hook's row types as written.

- [ ] **Step 2: Run test, verify failure**

Run: `yarn test --run src/api/materialize/dataflow/useDataflowGraphData.test.ts`
Expected: FAIL, module missing.

- [ ] **Step 3: Implement**

```typescript
import { sql } from "kysely";
import React from "react";

import { escapedLiteral as lit, useSqlManyTyped } from "~/api/materialize";
import {
  buildDataflowStructure,
  type ChannelRow,
  type DataflowStructure,
  type LirSpanRow,
  type OperatorRow,
} from "~/platform/dataflows/dataflowGraph";

import { queryBuilder } from "../db";

export interface DataflowGraphParams {
  clusterName: string;
  replicaName: string;
  dataflowId: string;
}

function dataflowIdLiteral(dataflowId: string) {
  if (!/^\d+$/.test(dataflowId)) {
    throw new Error(`invalid dataflow id: ${dataflowId}`);
  }
  return sql`${lit(dataflowId)}::uint8`;
}

export function useDataflowGraphData(params?: DataflowGraphParams) {
  const queries = React.useMemo(() => {
    if (!params) return null;
    const id = dataflowIdLiteral(params.dataflowId);
    return {
      operators: sql`
        SELECT
          mdod.id,
          mda.address,
          mdod.name,
          coalesce(mas.records, 0) AS "arrangementRecords",
          coalesce(mas.size, 0) AS "arrangementSize",
          coalesce(mse.elapsed_ns, 0) AS "elapsedNs"
        FROM mz_dataflow_operator_dataflows AS mdod
        JOIN mz_dataflow_addresses AS mda ON mda.id = mdod.id
        LEFT JOIN mz_arrangement_sizes AS mas ON mas.operator_id = mdod.id
        LEFT JOIN mz_scheduling_elapsed AS mse ON mse.id = mdod.id
        WHERE mdod.dataflow_id = ${id}`
        .$castTo<OperatorRow>()
        .compile(queryBuilder),
      channels: sql`
        SELECT
          mdco.id,
          from_operator_address AS "fromOperatorAddress",
          from_port AS "fromPort",
          to_operator_address AS "toOperatorAddress",
          to_port AS "toPort",
          COALESCE(sum(mmc.sent), 0) AS "messagesSent",
          COALESCE(sum(mmc.batch_sent), 0) AS "batchesSent",
          mdco.type AS "channelType"
        FROM mz_dataflow_channel_operators AS mdco
        LEFT JOIN mz_message_counts AS mmc ON mdco.id = mmc.channel_id
        WHERE from_operator_address[1] = ${id}
        GROUP BY
          mdco.id, "fromOperatorAddress", "fromPort",
          "toOperatorAddress", "toPort", mdco.type`
        .$castTo<ChannelRow>()
        .compile(queryBuilder),
      lirSpans: sql`
        SELECT
          mce.export_id AS "exportId",
          mlm.lir_id::text AS "lirId",
          mlm.operator,
          mlm.operator_id_start AS "operatorIdStart",
          mlm.operator_id_end AS "operatorIdEnd"
        FROM mz_lir_mapping AS mlm
        JOIN mz_compute_exports AS mce ON mlm.global_id = mce.export_id
        WHERE mce.dataflow_id = ${id}`
        .$castTo<LirSpanRow>()
        .compile(queryBuilder),
    };
  }, [params]);

  const { results, error, databaseError, loading, refetch } = useSqlManyTyped(queries, {
    cluster: params?.clusterName,
    replica: params?.replicaName,
    // This query can be slow for large dataflows.
    timeout: 30_000,
  });

  const key = params
    ? `${params.clusterName}|${params.replicaName}|${params.dataflowId}`
    : null;
  const [lastGood, setLastGood] = React.useState<{
    key: string;
    data: { structure: DataflowStructure; fetchedAt: Date };
  } | null>(null);

  // Tag successful results with the params key they were fetched for. Stale
  // in-flight responses for older params are aborted by useSqlMany, so a
  // result observed here belongs to the current key.
  React.useEffect(() => {
    if (key && results && !error && !loading) {
      setLastGood({
        key,
        data: {
          structure: buildDataflowStructure(
            results.operators ?? [],
            results.channels ?? [],
            results.lirSpans ?? [],
          ),
          fetchedAt: new Date(),
        },
      });
    }
  }, [key, results, error, loading]);

  // Error always wins, and data for other params never renders.
  const data = !error && lastGood && lastGood.key === key ? lastGood.data : null;
  return { data, error, databaseError, loading, refetch };
}
```

- [ ] **Step 4: Run test, verify pass**

Run: `yarn test --run src/api/materialize/dataflow/useDataflowGraphData.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add console/src/api/materialize/dataflow/
git commit -m "console: dataflow graph data hook keyed by dataflow id"
```

### Task 6: Nodes, edges, and DataflowGraphView

**Files:**
- Create: `console/src/platform/dataflows/nodes.tsx`
- Create: `console/src/platform/dataflows/ChannelEdge.tsx`
- Create: `console/src/platform/dataflows/DataflowGraphView.tsx`
- Create: `console/src/test/mockReactFlow.tsx`
- Modify: `console/src/platform/dataflows/dataflowGraph.ts` (add `GraphDecorations`, Step 0)

**Interfaces:**
- Consumes: `VisibleGraph`, `Positions`, `useElkLayout`, `NODE_DIMENSIONS`, `MAX_VISIBLE_NODES`, `visibleNodeCount`.
- Produces:

```typescript
export interface DataflowGraphViewProps {
  structure: DataflowStructure;
  collapsed: CollapseState;
  onCollapsedChange: (next: CollapseState) => void;
  cacheKey: string; // structure identity, owner appends collapse signature internally
  decorations?: GraphDecorations; // added in Task 13, optional from the start
  onNodeClick?: (node: VisibleNode) => void;
}
// Declared in dataflowGraph.ts (single source; DataflowGraphView imports it).
// Task 13 adds dimmedEdgeIds and searchMatches and makes decorateGraph return
// all fields; until then the view treats every field as optional.
export interface GraphDecorations {
  dimmedNodeIds?: ReadonlySet<string>;
  hiddenNodeIds?: ReadonlySet<string>;
  hiddenEdgeIds?: ReadonlySet<string>;
  dimmedEdgeIds?: ReadonlySet<string>;
  nodeColors?: ReadonlyMap<string, string>; // heatmap override
  searchMatches?: string[];
}
```

- [ ] **Step 0: Add GraphDecorations to dataflowGraph.ts**

Append to `console/src/platform/dataflows/dataflowGraph.ts` (all-optional here; Task 13's `decorateGraph` returns all fields set):

```typescript
export interface GraphDecorations {
  dimmedNodeIds?: ReadonlySet<string>;
  hiddenNodeIds?: ReadonlySet<string>;
  hiddenEdgeIds?: ReadonlySet<string>;
  dimmedEdgeIds?: ReadonlySet<string>;
  nodeColors?: ReadonlyMap<string, string>; // heatmap override
  searchMatches?: string[];
}
```

- [ ] **Step 1: Implement nodes.tsx**

Colors keep the current palette constants. All text via React, `formatBytesShort` from `~/utils/format`.

```tsx
import { Badge, Box, Text, Tooltip } from "@chakra-ui/react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import type { VisibleNode } from "./dataflowGraph";

export const COLORS = {
  noArrangementRegion: "#12b886",
  noArrangementOperator: "#ffffff",
  arrangementRegion: "#7950f2",
  arrangementOperator: "#fab005",
};

export function nodeFillColor(node: VisibleNode): string {
  const arranged = (node.transitive?.arrangementRecords ?? 0n) > 0n;
  const region = node.kind !== "operator" && node.kind !== "port";
  if (region) return arranged ? COLORS.arrangementRegion : COLORS.noArrangementRegion;
  return arranged ? COLORS.arrangementOperator : COLORS.noArrangementOperator;
}

export function formatElapsed(ns: bigint): string {
  return `scheduled ${Math.round(Number(ns) / 1e9)}s`;
}

export type FlowNodeData = { node: VisibleNode; dimmed: boolean; color: string };

const statLines = (node: VisibleNode) => {
  const lines: string[] = [];
  const stats = node.stats;
  if (!stats) return lines;
  if (stats.arrangementRecords > 0n) {
    lines.push(`${stats.arrangementRecords} arranged records`);
    lines.push(formatBytesShort(stats.arrangementSize));
  }
  if (stats.elapsedNs > 0n) lines.push(formatElapsed(stats.elapsedNs));
  return lines;
};

const CardShell = ({
  data,
  children,
}: {
  data: FlowNodeData;
  children: React.ReactNode;
}) => (
  <Box
    borderWidth="1px"
    borderRadius="md"
    px={2}
    py={1}
    width="100%"
    height="100%"
    overflow="hidden"
    background={data.color}
    opacity={data.dimmed ? 0.25 : 1}
  >
    {children}
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
  </Box>
);

export const OperatorNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <CardShell data={data}>
    <Tooltip
      label={data.node.lir.map((l) => `LIR ${l.lirId}: ${l.operator}`).join(", ")}
      isDisabled={data.node.lir.length === 0}
    >
      <Box>
        <Text fontSize="sm" fontWeight="600" noOfLines={1}>
          {data.node.label}
        </Text>
        {statLines(data.node).map((line) => (
          <Text key={line} fontSize="xs" noOfLines={1}>
            {line}
          </Text>
        ))}
      </Box>
    </Tooltip>
  </CardShell>
);

export const CollapsedRegionNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <CardShell data={data}>
    <Text fontSize="sm" fontWeight="600" noOfLines={1}>
      {data.node.label}
    </Text>
    <Badge fontSize="2xs">{data.node.childCount} children</Badge>
    {statLines(data.node).map((line) => (
      <Text key={line} fontSize="xs" noOfLines={1}>
        {line}
      </Text>
    ))}
  </CardShell>
);

export const RegionNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <Box
    borderWidth="2px"
    borderStyle="dashed"
    borderRadius="md"
    width="100%"
    height="100%"
    background={`${data.color}20`}
    opacity={data.dimmed ? 0.25 : 1}
  >
    <Text fontSize="sm" fontWeight="600" px={2} noOfLines={1}>
      {data.node.label}
    </Text>
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
  </Box>
);

export const PortNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <Box
    borderWidth="1px"
    borderRadius="full"
    px={2}
    background="gray.100"
    opacity={data.dimmed ? 0.25 : 1}
  >
    <Text fontSize="2xs">{data.node.label}</Text>
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
  </Box>
);

export const nodeTypes = {
  operator: OperatorNode,
  collapsedRegion: CollapsedRegionNode,
  region: RegionNode,
  port: PortNode,
};
```

- [ ] **Step 2: Implement ChannelEdge.tsx**

```tsx
import { Text, Tooltip } from "@chakra-ui/react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@xyflow/react";
import React from "react";

export type ChannelEdgeData = {
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
  dimmed: boolean;
};

export const ChannelEdge = (props: EdgeProps & { data: ChannelEdgeData }) => {
  const [path, labelX, labelY] = getBezierPath(props);
  const { messagesSent, batchesSent, channelTypes, dimmed } = props.data;
  const idle = messagesSent === 0n;
  const label = idle ? "" : `${messagesSent} records / ${batchesSent} batches`;
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          strokeDasharray: idle ? "6 4" : undefined,
          opacity: dimmed ? 0.15 : 1,
        }}
      />
      {label && (
        <EdgeLabelRenderer>
          <Tooltip label={channelTypes.join(", ") || "unknown channel type"}>
            <Text
              position="absolute"
              transform={`translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`}
              fontSize="2xs"
              background="whiteAlpha.800"
              px={1}
              borderRadius="sm"
              opacity={dimmed ? 0.15 : 1}
              className="nopan"
            >
              {label}
              {channelTypes.length > 0 && ` · ${channelTypes.join(", ")}`}
            </Text>
          </Tooltip>
        </EdgeLabelRenderer>
      )}
    </>
  );
};
```

- [ ] **Step 3: Implement DataflowGraphView.tsx**

```tsx
import { Box, Spinner, useToast } from "@chakra-ui/react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import React from "react";

import "@xyflow/react/dist/style.css";

import {
  deriveVisibleGraph,
  MAX_VISIBLE_NODES,
  visibleNodeCount,
  type CollapseState,
  type DataflowStructure,
  type GraphDecorations,
  type VisibleNode,
} from "./dataflowGraph";
import { ChannelEdge } from "./ChannelEdge";
import { NODE_DIMENSIONS } from "./elkGraph";
import { nodeFillColor, nodeTypes } from "./nodes";
import { useElkLayout } from "./useElkLayout";

const edgeTypes = { channel: ChannelEdge };

export interface DataflowGraphViewProps {
  structure: DataflowStructure;
  collapsed: CollapseState;
  onCollapsedChange: (next: CollapseState) => void;
  cacheKey: string;
  decorations?: GraphDecorations;
  onNodeClick?: (node: VisibleNode) => void;
}

export const DataflowGraphView = ({
  structure,
  collapsed,
  onCollapsedChange,
  cacheKey,
  decorations,
  onNodeClick,
}: DataflowGraphViewProps) => {
  const toast = useToast();
  const visible = React.useMemo(() => {
    const graph = deriveVisibleGraph(structure, collapsed);
    if (!decorations?.hiddenNodeIds && !decorations?.hiddenEdgeIds) return graph;
    const hiddenNodes = decorations.hiddenNodeIds ?? new Set();
    const hiddenEdges = decorations.hiddenEdgeIds ?? new Set();
    return {
      nodes: graph.nodes.filter((n) => !hiddenNodes.has(n.id)),
      edges: graph.edges.filter(
        (e) =>
          !hiddenEdges.has(e.id) && !hiddenNodes.has(e.source) && !hiddenNodes.has(e.target),
      ),
    };
  }, [structure, collapsed, decorations?.hiddenNodeIds, decorations?.hiddenEdgeIds]);

  const layoutKey = `${cacheKey}|${[...collapsed].sort().join(",")}|${
    decorations?.hiddenNodeIds ? [...decorations.hiddenNodeIds].sort().join(",") : ""
  }`;
  const { positions, layouting, error } = useElkLayout(visible, layoutKey);

  const toggleRegion = React.useCallback(
    (node: VisibleNode) => {
      const next = new Set(collapsed);
      if (next.has(node.id)) {
        next.delete(node.id);
        if (visibleNodeCount(structure, next) > MAX_VISIBLE_NODES) {
          toast({
            status: "warning",
            title: `Expanding would show more than ${MAX_VISIBLE_NODES} nodes.`,
          });
          return;
        }
      } else {
        next.add(node.id);
      }
      onCollapsedChange(next);
    },
    [collapsed, onCollapsedChange, structure, toast],
  );

  const nodes: Node[] = React.useMemo(() => {
    if (!positions) return [];
    return visible.nodes.map((n) => {
      const pos = positions[n.id] ?? { x: 0, y: 0, ...NODE_DIMENSIONS[n.kind] };
      return {
        id: n.id,
        type: n.kind,
        position: { x: pos.x, y: pos.y },
        parentId: n.parent ?? undefined,
        extent: n.parent ? ("parent" as const) : undefined,
        style: { width: pos.width, height: pos.height },
        draggable: false,
        connectable: false,
        data: {
          node: n,
          dimmed: decorations?.dimmedNodeIds?.has(n.id) ?? false,
          color: decorations?.nodeColors?.get(n.id) ?? nodeFillColor(n),
        },
      };
    });
  }, [visible, positions, decorations?.dimmedNodeIds, decorations?.nodeColors]);

  const edges: Edge[] = React.useMemo(
    () =>
      visible.edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        type: "channel",
        data: {
          messagesSent: e.messagesSent,
          batchesSent: e.batchesSent,
          channelTypes: e.channelTypes,
          dimmed:
            (decorations?.dimmedNodeIds?.has(e.source) ?? false) ||
            (decorations?.dimmedNodeIds?.has(e.target) ?? false) ||
            (decorations?.dimmedEdgeIds?.has(e.id) ?? false),
        },
      })),
    [visible, decorations?.dimmedNodeIds],
  );

  if (error) throw new Error(error);
  return (
    <Box width="100%" flex="1" position="relative">
      {layouting && (
        <Box position="absolute" top={2} right={2} zIndex={10}>
          <Spinner size="sm" />
        </Box>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onlyRenderVisibleElements
        fitView
        minZoom={0.05}
        onNodeClick={(_, node) => onNodeClick?.((node.data as { node: VisibleNode }).node)}
        onNodeDoubleClick={(_, node) => {
          const visibleNode = (node.data as { node: VisibleNode }).node;
          if (visibleNode.kind === "region" || visibleNode.kind === "collapsedRegion") {
            toggleRegion(visibleNode);
          }
        }}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap pannable zoomable />
      </ReactFlow>
    </Box>
  );
};
```

- [ ] **Step 4: Create the shared React Flow test mock**

`console/src/test/mockReactFlow.tsx`. jsdom lacks `ResizeObserver`/`DOMMatrixReadOnly`, so component tests mock `@xyflow/react` to a flat renderer that preserves the decision logic under test (which nodes and labels exist):

```tsx
import { vi } from "vitest";
import React from "react";

export function mockReactFlow() {
  vi.mock("@xyflow/react", () => ({
    ReactFlow: ({ nodes, children }: { nodes: { id: string; data: { node: { label: string } } }[]; children?: React.ReactNode }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <div key={n.id} data-testid={`node-${n.id}`}>
            {n.data.node.label}
          </div>
        ))}
        {children}
      </div>
    ),
    Background: () => null,
    Controls: () => null,
    MiniMap: () => null,
    Handle: () => null,
    Position: { Left: "left", Right: "right" },
    BaseEdge: () => null,
    EdgeLabelRenderer: ({ children }: { children: React.ReactNode }) => <>{children}</>,
    getBezierPath: () => ["", 0, 0],
  }));
}
```

Also mock `./useElkLayout` in component tests to return deterministic positions synchronously:

```typescript
vi.mock("~/platform/dataflows/useElkLayout", () => ({
  useElkLayout: (graph: { nodes: { id: string }[] } | null) => ({
    positions: graph
      ? Object.fromEntries(graph.nodes.map((n, i) => [n.id, { x: i * 100, y: 0, width: 100, height: 50 }]))
      : null,
    layouting: false,
    error: null,
  }),
}));
```

- [ ] **Step 5: Typecheck, lint, commit**

Run: `yarn typecheck && yarn lint`
Expected: PASS.

```bash
git add console/src/platform/dataflows/ console/src/test/mockReactFlow.tsx
git commit -m "console: React Flow dataflow graph view with collapsible regions"
```

### Task 7: Minimal detail page and route

**Files:**
- Create: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Modify: `console/src/platform/clusters/ClusterDetail.tsx`

**Interfaces:**
- Consumes: `useDataflowGraphData`, `DataflowGraphView`, `defaultCollapseState`, cluster store (`useAllClusters` pattern from `DataflowVisualizer.tsx:261-267`), `LabeledSelect`, `ErrorBox`, `MainContentContainer`.
- Produces: route `dataflows/:dataflowId` under cluster detail, replica via `?replica=` search param. Page component signature: `const DataflowDetailPage: () => JSX.Element` (default export).

- [ ] **Step 1: Implement DataflowDetailPage (minimal: replica select, graph, spinner, error box)**

```tsx
import { Alert, AlertIcon, Spinner, Text, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { ErrorCode } from "~/api/materialize/types";
import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";

import {
  defaultCollapseState,
  type CollapseState,
} from "./dataflowGraph";
import { DataflowGraphView } from "./DataflowGraphView";

const DataflowDetailPage = () => {
  const { clusterId, dataflowId } = useParams();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  const replicaName = searchParams.get("replica") ?? cluster?.replicas[0]?.name;

  const params = React.useMemo(
    () =>
      cluster && replicaName && dataflowId
        ? { clusterName: cluster.name, replicaName, dataflowId }
        : undefined,
    [cluster, replicaName, dataflowId],
  );
  const { data, error, databaseError, loading } = useDataflowGraphData(params);

  const [collapsed, setCollapsed] = React.useState<CollapseState | null>(null);
  const structureKey = data
    ? `${params?.dataflowId}/${params?.replicaName}/${data.structure.nodes.size}`
    : null;
  React.useEffect(() => {
    if (data) setCollapsed(defaultCollapseState(data.structure));
    // Reset collapse state when the structure identity changes.
  }, [structureKey]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!cluster) return null;
  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;

  return (
    <MainContentContainer width="100%">
      <VStack width="100%" height="100%" alignItems="stretch">
        <LabeledSelect
          label="Replica"
          value={replicaName ?? ""}
          onChange={(e) => setSearchParams({ replica: e.target.value })}
          flexShrink={0}
        >
          {cluster.replicas.map((r) => (
            <option key={r.name} value={r.name}>
              {r.name}
            </option>
          ))}
        </LabeledSelect>
        {permissionError ? (
          <Alert status="info" rounded="md" p={4} width="auto">
            <AlertIcon />
            <Text>
              You&apos;ll need{" "}
              <Text as="span" textStyle="monospace">
                USAGE
              </Text>{" "}
              privilege on this cluster to visualize this dataflow.
            </Text>
          </Alert>
        ) : error ? (
          <ErrorBox message="There was an error visualizing your dataflow" />
        ) : !data || !collapsed ? (
          <Spinner />
        ) : data.structure.nodes.size <= 1 ? (
          <Text>This dataflow contains no operators.</Text>
        ) : (
          <DataflowGraphView
            structure={data.structure}
            collapsed={collapsed}
            onCollapsedChange={setCollapsed}
            cacheKey={structureKey ?? ""}
          />
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default DataflowDetailPage;
```

- [ ] **Step 2: Wire the route**

In `console/src/platform/clusters/ClusterDetail.tsx`: lazy-import the page, add the route inside `SentryRoutes` (`ClusterDetail.tsx:149-162`), and add the tab to `subnavItems` (`ClusterDetail.tsx:127-137`) gated on the flag. Use the same `useFlags` import that `SimpleObjectDetailRoutes.tsx` uses (check its import block and copy it).

```tsx
const DataflowDetailPage = React.lazy(
  () => import("~/platform/dataflows/DataflowDetailPage"),
);
```

```tsx
const flags = useFlags();
const subnavItems: Tab[] = React.useMemo(() => {
  const tabs: Tab[] = [
    { label: "Overview", href: "..", end: true },
    { label: "Replicas", href: "../replicas" },
    { label: "Materialized Views", href: "../materialized-views" },
    { label: "Indexes", href: "../indexes" },
    { label: "Sources", href: "../sources" },
    { label: "Sinks", href: "../sinks" },
  ];
  if (flags["visualization-features"]) {
    tabs.push({ label: "Dataflows", href: "../dataflows" });
  }
  return tabs;
}, [flags]);
```

```tsx
<Route
  path="dataflows/:dataflowId"
  element={<DataflowDetailPage key={clusterName} />}
/>
```

(The list route at `path="dataflows"` comes in Task 9. Until then, navigate by URL directly.)

- [ ] **Step 3: Manual smoke test**

Start a local Materialize (`bin/environmentd` from repo root; see `mz-run` skill if it fails) and the console dev server (`cd console && yarn start`). Create a table plus an index, find its dataflow id (`SELECT id, name FROM mz_introspection.mz_dataflows`, run with `SET cluster = ...`), and open `https://local.dev.materialize.com:3000/.../clusters/<id>/<name>/dataflows/<dataflowId>`. Expected: graph renders, regions expand and collapse on double click.

- [ ] **Step 4: Typecheck, lint, commit**

```bash
yarn typecheck && yarn lint
git add console/src
git commit -m "console: dataflow detail page with graph view behind flag"
```

### Task 8: CHECKPOINT, phase 1 gate

**Execution mode:** driver-executed. This task needs a browser (devtools performance panel, screenshots) and a local environmentd, so the main session or a human runs it. Do NOT dispatch it to a code subagent.

**Files:** none committed. Temporary `performance.mark` instrumentation in `useElkLayout.ts` around postMessage/response is permitted for Step 2 and must be reverted before Task 9 (`git status` clean afterwards).

STOP after this task and report to the user.

- [ ] **Step 1: Create the reference dataflow**

Against local environmentd (`psql -h localhost -p 6875 -U materialize`):

```sql
CREATE TABLE orders (id int, cust int, amt int);
CREATE TABLE customers (id int, region int);
CREATE TABLE regions (id int, name text);
CREATE TABLE items (order_id int, sku int, qty int);
CREATE TABLE skus (id int, price int);
CREATE MATERIALIZED VIEW gate_mv AS
SELECT r.name, count(*) AS orders, sum(i.qty * s.price) AS revenue
FROM orders o
JOIN customers c ON o.cust = c.id
JOIN regions r ON c.region = r.id
JOIN items i ON i.order_id = o.id
JOIN skus s ON s.id = i.sku
GROUP BY r.name
HAVING sum(i.qty * s.price) > 100;
INSERT INTO regions VALUES (1, 'emea'), (2, 'amer');
INSERT INTO customers SELECT g, 1 + g % 2 FROM generate_series(1, 100) g;
INSERT INTO orders SELECT g, 1 + g % 100, g FROM generate_series(1, 1000) g;
INSERT INTO skus SELECT g, g FROM generate_series(1, 50) g;
INSERT INTO items SELECT g % 1000 + 1, g % 50 + 1, g FROM generate_series(1, 5000) g;
```

Verify operator count: `SET cluster = quickstart; SELECT count(*) FROM mz_introspection.mz_dataflow_operator_dataflows WHERE dataflow_name LIKE '%gate_mv%';`
Expected: >= 300. If under 300, add a sixth join (`JOIN customers c2 ON c2.id = o.cust`) and re-check.

- [ ] **Step 2: Measure**

Open the gate_mv dataflow in the visualizer (dev server). In browser devtools performance panel or via `performance.mark` temporarily added around the `useElkLayout` postMessage/response, record:

* Default collapsed view layout time. Gate: < 500 ms.
* Expand regions until visible node count approaches 1500 (temporarily raise expansion via expand-all in console, or expand largest regions). Layout time. Gate: < 5 s, UI responsive (canvas pans during layout).
* Repeat both in a production build: `yarn build:local && yarn preview`, same URL on port 3000. Gate: worker loads and lays out in the built bundle.

- [ ] **Step 3: Visual check**

Screenshots of collapsed and expanded gate_mv. Gate: no overlapping nodes, edges flow left to right.

- [ ] **Step 4: STOP, report**

Report measurements + screenshots to the user for sign-off (spec MVP section). If any gate fails: do not proceed, return to the design doc per spec.

---

## Phase 2: selection and refresh

### Task 9: Dataflow list hook and DataflowsPage

**Files:**
- Create: `console/src/api/materialize/dataflow/useDataflowList.ts`
- Create: `console/src/platform/dataflows/DataflowsPage.tsx`
- Modify: `console/src/platform/clusters/ClusterDetail.tsx`
- Test: `console/src/api/materialize/dataflow/useDataflowList.test.ts`

**Interfaces:**
- Produces:

```typescript
export interface DataflowListEntry {
  id: string; // uint8 as string
  name: string;
  records: bigint;
  size: bigint;
  elapsedNs: bigint;
}
export function useDataflowList(params?: { clusterName: string; replicaName: string }): {
  data: DataflowListEntry[] | null;
  error: unknown; databaseError: unknown; loading: boolean; refetch: () => void;
};
```

- [ ] **Step 1: Write failing hook test** (msw, same harness as Task 5): mock one row, assert mapping to `DataflowListEntry` with bigint fields normalized via `BigInt()`, assert `data` is null while `error` set.

```typescript
const listResult = {
  desc: { columns: [{ name: "id" }, { name: "name" }, { name: "records" }, { name: "size" }, { name: "elapsedNs" }] },
  rows: [["7", "Dataflow: mv", "100", "4096", "12345"]],
};
// handler returns { results: [listResult] }; assert
// result.current.data?.[0] === { id: "7", name: "Dataflow: mv", records: 100n, size: 4096n, elapsedNs: 12345n }
```

- [ ] **Step 2: Verify failure, then implement**

Query (kysely `sql`, `.$castTo<...>()`, same pattern as Task 5, no literals so no escaping):

```sql
SELECT
  md.id::text AS id,
  md.name,
  COALESCE(mrpd.records, 0) AS records,
  COALESCE(mrpd.size, 0) AS size,
  COALESCE(el.elapsed_ns, 0) AS "elapsedNs"
FROM mz_dataflows AS md
LEFT JOIN mz_records_per_dataflow AS mrpd ON mrpd.id = md.id
LEFT JOIN (
  SELECT mdod.dataflow_id, sum(mse.elapsed_ns) AS elapsed_ns
  FROM mz_dataflow_operator_dataflows AS mdod
  JOIN mz_scheduling_elapsed AS mse ON mse.id = mdod.id
  GROUP BY mdod.dataflow_id
) AS el ON el.dataflow_id = md.id
ORDER BY size DESC
```

Hook body mirrors `useDataflowGraphData` minus tagging (a list going momentarily stale across replica switch is masked by `loading`, and errors clear `data` the same way: `const data = !error && results ? normalize(results.list) : null`).

- [ ] **Step 3: Implement DataflowsPage**

Loading spinner, `ErrorBox` on error, permission alert exactly as in Task 7. Export-deep-link resolution is Task 10, leave a plain list here. Core:

```tsx
const DataflowsPage = () => {
  const { clusterId } = useParams();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  const replicaName = searchParams.get("replica") ?? cluster?.replicas[0]?.name;
  const params = React.useMemo(
    () =>
      cluster && replicaName ? { clusterName: cluster.name, replicaName } : undefined,
    [cluster, replicaName],
  );
  const { data, error, databaseError, loading } = useDataflowList(params);
  if (!cluster) return null;
  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;
  return (
    <MainContentContainer width="100%">
      <VStack alignItems="stretch">
        <LabeledSelect
          label="Replica"
          value={replicaName ?? ""}
          onChange={(e) => setSearchParams({ replica: e.target.value })}
        >
          {cluster.replicas.map((r) => (
            <option key={r.name} value={r.name}>{r.name}</option>
          ))}
        </LabeledSelect>
        {permissionError ? (
          <Alert status="info" rounded="md" p={4} width="auto">
            <AlertIcon />
            <Text>
              You&apos;ll need{" "}
              <Text as="span" textStyle="monospace">
                USAGE
              </Text>{" "}
              privilege on this cluster to list its dataflows.
            </Text>
          </Alert>
        ) : error ? (
          <ErrorBox message="There was an error listing dataflows" />
        ) : !data && loading ? (
          <Spinner />
        ) : (
        <Table size="sm">
          <Thead>
            <Tr><Th>Name</Th><Th isNumeric>Records</Th><Th isNumeric>Size</Th><Th isNumeric>Scheduled</Th></Tr>
          </Thead>
          <Tbody>
            {(data ?? []).map((d) => (
              <Tr key={d.id}>
                <Td>
                  <Link to={`${d.id}?replica=${replicaName}`}>{d.name}</Link>
                </Td>
                <Td isNumeric>{d.records.toString()}</Td>
                <Td isNumeric>{formatBytesShort(d.size)}</Td>
                <Td isNumeric>{Math.round(Number(d.elapsedNs) / 1e9)}s</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
        )}
      </VStack>
    </MainContentContainer>
  );
};
export default DataflowsPage;
```

- [ ] **Step 4: Wire route**

`ClusterDetail.tsx`: `<Route path="dataflows" element={<DataflowsPage key={clusterName} />} />` next to the Task 7 route (lazy import, flag-gated same as tab).

- [ ] **Step 5: Test, typecheck, manual check list renders, commit**

```bash
yarn test --run src/api/materialize/dataflow/ && yarn typecheck && yarn lint
git add console/src
git commit -m "console: cluster dataflows list page"
```

### Task 10: Export deep-link and object tab rewire

**Files:**
- Create: `console/src/api/materialize/dataflow/useDataflowIdForExport.ts`
- Modify: `console/src/platform/dataflows/DataflowsPage.tsx`
- Modify: `console/src/platform/object-explorer/SimpleObjectDetailRoutes.tsx`

**Interfaces:**
- Produces: `useDataflowIdForExport(params?: { clusterName: string; replicaName: string; exportId: string }): { dataflowId: string | null; loading: boolean; error: unknown }`. Query: `SELECT dataflow_id::text AS "dataflowId" FROM mz_compute_exports WHERE export_id = ${lit(exportId)}` (exportId validated `/^[stu]\d+$/`).

- [ ] **Step 1: Implement hook** (pattern of Task 9, single query, `dataflowId = results?.rows?.[0]?.dataflowId ?? null`).

- [ ] **Step 2: DataflowsPage export resolution**

When `?export=<id>` is present and a replica is chosen: call the hook, on `dataflowId` render `<Navigate to={`${dataflowId}?replica=${replicaName}`} replace />`. On resolved-but-null (export gone): show `ErrorBox` message "This object has no running dataflow on the selected replica." with the list rendered below.

- [ ] **Step 3: Rewire the object "Visualize" tab**

In `SimpleObjectDetailRoutes.tsx`: keep `shouldShowDataflowVisualizer` logic (`:136-139`). Replace the tab href (`:160-165`) and the route (`:203-205`): the tab now links to the cluster page. The object row has `clusterId`; get the cluster name via `useAllClusters().getClusterById`. Href: `/clusters/${object.clusterId}/${clusterName}/dataflows?export=${object.id}`. Verify the absolute-path shape against how `ClustersList.tsx` builds cluster links and match it (react-router basename handles the prefix). Delete the `dataflow-visualizer` Route and the `DataflowVisualizer` lazy import (`:37-39`). The old component file itself is deleted in Task 18.

- [ ] **Step 4: Manual check** Object detail → Visualize → lands on graph of that object's dataflow. Typecheck, lint, commit.

```bash
git add console/src
git commit -m "console: deep-link object visualize tab into dataflows page"
```

### Task 11: Refresh in place

**Files:**
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`

**Interfaces:**
- Consumes: `refetch` and `data.fetchedAt` from `useDataflowGraphData`.

- [ ] **Step 1: Add refresh UI**

Toolbar row above the graph: `Button` "Refresh" calling `refetch`, disabled while `loading`. `Text` "Last fetched {fetchedAt.toLocaleTimeString()}". While a same-params refetch is loading, `data` stays non-null (Task 5 contract), so the graph stays mounted and stats update in place when the new result lands.

- [ ] **Step 2: Preserve layout across refresh**

`structureKey` in Task 7 already encodes `nodes.size`. Strengthen it so pure stats refreshes reuse layout and structure changes relayout: `structureKey = `${dataflowId}/${replicaName}/` + a stable digest of sorted node ids` (join sorted `[...structure.nodes.keys()]` with "," and use its length + a simple 32-bit string hash, implemented inline as `hashString`). Collapse state resets only when `structureKey` changes, which was already the effect dependency.

```typescript
function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}
```

- [ ] **Step 3: Manual check** Refresh keeps viewport + expansion, stats badges update. Typecheck, lint, commit.

```bash
git add console/src
git commit -m "console: manual refresh preserving dataflow graph layout"
```

---

## Phase 3: interactions

### Task 12: Node detail panel

**Files:**
- Create: `console/src/platform/dataflows/NodeDetailPanel.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Modify: `console/src/platform/dataflows/DataflowGraphView.tsx` (add `onPaneClick` pass-through prop)

- [ ] **Step 1: Implement panel**

```tsx
export const NodeDetailPanel = ({
  node,
  onClose,
}: {
  node: VisibleNode;
  onClose: () => void;
}) => {
  const row = (label: string, value: string) => (
    <HStack key={label} justifyContent="space-between">
      <Text fontSize="xs" color="gray.500">{label}</Text>
      <Text fontSize="xs" textStyle="monospace">{value}</Text>
    </HStack>
  );
  return (
    <Box width="320px" borderLeftWidth="1px" p={3} overflowY="auto" flexShrink={0}>
      <HStack justifyContent="space-between">
        <Text fontWeight="600" noOfLines={2}>{node.label}</Text>
        <CloseButton onClick={onClose} />
      </HStack>
      {row("Kind", node.kind)}
      {node.address && row("Address", node.address.join("."))}
      {node.childCount > 0 && row("Children", String(node.childCount))}
      {node.stats && (
        <>
          {row("Arranged records", node.stats.arrangementRecords.toString())}
          {row("Arrangement size", formatBytesShort(node.stats.arrangementSize))}
          {row("Elapsed", `${Math.round(Number(node.stats.elapsedNs) / 1e9)}s`)}
        </>
      )}
      {node.transitive && node.childCount > 0 && (
        <>
          {row("Subtree records", node.transitive.arrangementRecords.toString())}
          {row("Subtree size", formatBytesShort(node.transitive.arrangementSize))}
          {row("Subtree elapsed", `${Math.round(Number(node.transitive.elapsedNs) / 1e9)}s`)}
        </>
      )}
      {node.lir.map((l) => (
        <Text key={`${l.exportId}/${l.lirId}`} fontSize="xs" mt={2}>
          LIR {l.lirId} ({l.exportId}): {l.operator}
        </Text>
      ))}
    </Box>
  );
};
```

- [ ] **Step 2: Wire** `onNodeClick` in `DataflowDetailPage` sets `selectedNode` state; render panel beside the graph in an `HStack`. Clicking the canvas background clears it: `DataflowGraphViewProps` gains `onPaneClick?: () => void;`, forwarded as `<ReactFlow onPaneClick={onPaneClick} ...>`.

- [ ] **Step 3: Typecheck, lint, manual check, commit**

```bash
git add console/src
git commit -m "console: node detail panel for dataflow visualizer"
```

### Task 13: Filters model + toolbar (search, hide idle, channel types)

**Files:**
- Modify: `console/src/platform/dataflows/dataflowGraph.ts` (decorations, pure)
- Create: `console/src/platform/dataflows/DataflowToolbar.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Modify: `console/src/platform/dataflows/DataflowGraphView.tsx` (`centerRef` prop, `CenterHelper`)
- Test: `console/src/platform/dataflows/dataflowGraph.test.ts`

**Interfaces:**
- Produces (in `dataflowGraph.ts`):

```typescript
export interface Filters {
  search: string;
  hideIdle: boolean;
  heatmap: "off" | "elapsed" | "size";
  heatmapThreshold: number; // 0..1 fraction of max
  channelTypes: string[] | null; // null = all
}
export const DEFAULT_FILTERS: Filters;

// Replaces the all-optional Task 6 declaration in dataflowGraph.ts. decorateGraph
// returns every field set. DataflowGraphView's optional-chaining reads and its
// optional `decorations` prop remain valid against the narrowed type.
export interface GraphDecorations {
  dimmedNodeIds: Set<string>;
  hiddenNodeIds: Set<string>;
  hiddenEdgeIds: Set<string>;
  dimmedEdgeIds: Set<string>;
  nodeColors: Map<string, string>;
  searchMatches: string[]; // visible node ids, document order
}
export function decorateGraph(
  graph: VisibleGraph,
  filters: Filters,
  heatColor: (t: number) => string, // injected so the pure module stays d3-free
): GraphDecorations;

export function allChannelTypes(structure: DataflowStructure): string[];

// Search may match inside collapsed regions. Returns a collapse state with
// every ancestor of every matching node expanded (respecting nothing else).
export function expandForSearch(
  structure: DataflowStructure,
  collapsed: CollapseState,
  search: string,
): CollapseState;
```

Semantics (each a test):
* `search` non-empty: case-insensitive substring on `label`. Non-matching operator/collapsedRegion nodes → `dimmedNodeIds`. Matches listed in `searchMatches`.
* `hideIdle`: operators with `stats.elapsedNs === 0n` and no arranged records → `hiddenNodeIds`; edges with `messagesSent === 0n` → `hiddenEdgeIds`. Regions and ports never hidden.
* `channelTypes` non-null: edges whose `channelTypes` don't intersect → `hiddenEdgeIds` additions? No: dimmed, per spec. Put them in a dim path: edges get dimmed via node dimming today, so extend `ChannelEdgeData.dimmed` computation in `DataflowGraphView` with `hiddenEdgeIds`-independent `dimmedEdgeIds: Set<string>` added to `GraphDecorations` and populated here.
* `heatmap` != off: for every non-port node, `t = Number(metric(node)) / Number(max over visible non-port nodes)` with `metric = transitive.elapsedNs | transitive.arrangementSize`; `nodeColors.set(id, heatColor(t))`; nodes with `t < heatmapThreshold` → `dimmedNodeIds`. If max is 0, no colors set (spec: neutral base, slider disabled).

- [ ] **Step 1: Write failing tests**

Append to `dataflowGraph.test.ts`:

```typescript
import {
  allChannelTypes,
  decorateGraph,
  DEFAULT_FILTERS,
  expandForSearch,
} from "./dataflowGraph";

describe("decorateGraph", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  const expanded = deriveVisibleGraph(s, new Set());
  const heat = (t: number) => `heat(${t.toFixed(2)})`;

  it("search dims non-matching leaf nodes and lists matches", () => {
    const d = decorateGraph(expanded, { ...DEFAULT_FILTERS, search: "join" }, heat);
    expect(d.searchMatches).toEqual([nodeIdOf([5, 1, 1])]);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true);
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 1, 1]))).toBe(false);
  });

  it("hideIdle hides zero-activity operators and zero-message edges, never regions or ports", () => {
    const d = decorateGraph(expanded, { ...DEFAULT_FILTERS, hideIdle: true }, heat);
    // every fixture operator has elapsed > 0 except none; hide check via a synthetic idle op
    const idle = buildDataflowStructure(
      [...OPS, { id: "15", address: ["5", "3"], name: "Idle", arrangementRecords: "0", arrangementSize: "0", elapsedNs: "0" }],
      CHANNELS, [],
    );
    const d2 = decorateGraph(deriveVisibleGraph(idle, new Set()), { ...DEFAULT_FILTERS, hideIdle: true }, heat);
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 3]))).toBe(true);
    expect(d2.hiddenNodeIds.has(nodeIdOf([5, 1]))).toBe(false);
    expect(d.hiddenEdgeIds.size).toBe(0); // all fixture edges have messages
  });

  it("channel type filter dims non-matching edges", () => {
    const d = decorateGraph(expanded, { ...DEFAULT_FILTERS, channelTypes: ["batches"] }, heat);
    const rowsEdge = expanded.edges.find((e) => e.channelTypes.includes("rows"))!;
    const batchesEdge = expanded.edges.find((e) => e.channelTypes.includes("batches"))!;
    expect(d.dimmedEdgeIds.has(rowsEdge.id)).toBe(true);
    expect(d.dimmedEdgeIds.has(batchesEdge.id)).toBe(false);
  });

  it("heatmap colors by transitive metric and dims below threshold", () => {
    const d = decorateGraph(
      expanded,
      { ...DEFAULT_FILTERS, heatmap: "elapsed", heatmapThreshold: 0.5 },
      heat,
    );
    // max transitive elapsed among visible non-ports is the region (13n)
    expect(d.nodeColors.get(nodeIdOf([5, 1]))).toEqual("heat(1.00)");
    expect(d.dimmedNodeIds.has(nodeIdOf([5, 2]))).toBe(true); // 2/13 < 0.5
  });

  it("heatmap with all-zero metric sets no colors", () => {
    const zero = buildDataflowStructure(
      OPS.map((o) => ({ ...o, elapsedNs: "0" })), CHANNELS, [],
    );
    const d = decorateGraph(
      deriveVisibleGraph(zero, new Set()),
      { ...DEFAULT_FILTERS, heatmap: "elapsed" },
      heat,
    );
    expect(d.nodeColors.size).toBe(0);
  });
});

describe("expandForSearch / allChannelTypes", () => {
  const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
  it("expands ancestors of matches", () => {
    const next = expandForSearch(s, defaultCollapseState(s), "join");
    expect(next.has(nodeIdOf([5, 1]))).toBe(false);
  });
  it("collects channel types", () => {
    expect(allChannelTypes(s)).toEqual(["batches", "rows"]);
  });
});
```

- [ ] **Step 2: Implement**

```typescript
export const DEFAULT_FILTERS: Filters = {
  search: "",
  hideIdle: false,
  heatmap: "off",
  heatmapThreshold: 0,
  channelTypes: null,
};

export function allChannelTypes(structure: DataflowStructure): string[] {
  const types = new Set<string>();
  for (const c of structure.channels) if (c.channelType) types.add(c.channelType);
  return [...types].sort();
}

export function decorateGraph(
  graph: VisibleGraph,
  filters: Filters,
  heatColor: (t: number) => string,
): GraphDecorations {
  const d: GraphDecorations = {
    dimmedNodeIds: new Set(),
    hiddenNodeIds: new Set(),
    hiddenEdgeIds: new Set(),
    dimmedEdgeIds: new Set(),
    nodeColors: new Map(),
    searchMatches: [],
  };
  const needle = filters.search.trim().toLowerCase();
  for (const n of graph.nodes) {
    if (needle && n.kind !== "port" && n.kind !== "region") {
      if (n.label.toLowerCase().includes(needle)) d.searchMatches.push(n.id);
      else d.dimmedNodeIds.add(n.id);
    }
    if (
      filters.hideIdle &&
      n.kind === "operator" &&
      n.stats !== null &&
      n.stats.elapsedNs === 0n &&
      n.stats.arrangementRecords === 0n
    ) {
      d.hiddenNodeIds.add(n.id);
    }
  }
  for (const e of graph.edges) {
    if (filters.hideIdle && e.messagesSent === 0n) d.hiddenEdgeIds.add(e.id);
    if (
      filters.channelTypes !== null &&
      !e.channelTypes.some((t) => filters.channelTypes!.includes(t))
    ) {
      d.dimmedEdgeIds.add(e.id);
    }
  }
  if (filters.heatmap !== "off") {
    const metric = (n: VisibleNode): bigint =>
      filters.heatmap === "elapsed"
        ? n.transitive?.elapsedNs ?? 0n
        : n.transitive?.arrangementSize ?? 0n;
    const candidates = graph.nodes.filter((n) => n.kind !== "port");
    const max = candidates.reduce((m, n) => (metric(n) > m ? metric(n) : m), 0n);
    if (max > 0n) {
      for (const n of candidates) {
        const t = Number(metric(n)) / Number(max);
        d.nodeColors.set(n.id, heatColor(t));
        if (t < filters.heatmapThreshold) d.dimmedNodeIds.add(n.id);
      }
    }
  }
  return d;
}

export function expandForSearch(
  structure: DataflowStructure,
  collapsed: CollapseState,
  search: string,
): CollapseState {
  const needle = search.trim().toLowerCase();
  if (!needle) return collapsed;
  const next = new Set(collapsed);
  for (const node of structure.nodes.values()) {
    if (!node.name.toLowerCase().includes(needle)) continue;
    for (let len = 1; len < node.address.length; len++) {
      next.delete(nodeIdOf(node.address.slice(0, len)));
    }
  }
  return next;
}
```


- [ ] **Step 3: Verify tests pass**

Run: `yarn test --run src/platform/dataflows/dataflowGraph.test.ts`

- [ ] **Step 4: Toolbar UI**

Pure controlled component:

```tsx
export interface DataflowToolbarProps {
  filters: Filters;
  onFiltersChange: (next: Filters) => void;
  channelTypes: string[];
  matchCount: number;
  matchIndex: number;
  onJump: (delta: 1 | -1) => void;
}

export const DataflowToolbar = ({
  filters,
  onFiltersChange,
  channelTypes,
  matchCount,
  matchIndex,
  onJump,
}: DataflowToolbarProps) => {
  const [search, setSearch] = React.useState(filters.search);
  // Debounce search input into the filters object.
  React.useEffect(() => {
    if (search === filters.search) return;
    const timeout = setTimeout(() => onFiltersChange({ ...filters, search }), 300);
    return () => clearTimeout(timeout);
  }, [search, filters, onFiltersChange]);
  return (
    <HStack spacing={4} flexShrink={0} flexWrap="wrap">
      <Input
        size="sm"
        width="240px"
        placeholder="Search operators"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      {filters.search && (
        <HStack spacing={1}>
          <Button size="xs" onClick={() => onJump(-1)} isDisabled={matchCount === 0}>
            Prev
          </Button>
          <Button size="xs" onClick={() => onJump(1)} isDisabled={matchCount === 0}>
            Next
          </Button>
          <Text fontSize="xs">
            {matchCount === 0 ? "0/0" : `${matchIndex + 1}/${matchCount}`}
          </Text>
        </HStack>
      )}
      <FormControl display="flex" alignItems="center" width="auto">
        <FormLabel fontSize="xs" mb={0}>
          Hide idle
        </FormLabel>
        <Switch
          size="sm"
          isChecked={filters.hideIdle}
          onChange={(e) => onFiltersChange({ ...filters, hideIdle: e.target.checked })}
        />
      </FormControl>
      <Select
        size="sm"
        width="180px"
        value={filters.heatmap}
        onChange={(e) =>
          onFiltersChange({ ...filters, heatmap: e.target.value as Filters["heatmap"] })
        }
      >
        <option value="off">Heatmap off</option>
        <option value="elapsed">Heat: elapsed</option>
        <option value="size">Heat: arrangement size</option>
      </Select>
      <Slider
        width="120px"
        isDisabled={filters.heatmap === "off"}
        min={0}
        max={1}
        step={0.05}
        value={filters.heatmapThreshold}
        onChange={(v) => onFiltersChange({ ...filters, heatmapThreshold: v })}
      >
        <SliderTrack>
          <SliderFilledTrack />
        </SliderTrack>
        <SliderThumb />
      </Slider>
      <Menu closeOnSelect={false}>
        <MenuButton as={Button} size="sm">
          Channel types
        </MenuButton>
        <MenuList>
          {channelTypes.map((t) => (
            <MenuItem key={t}>
              <Checkbox
                isChecked={filters.channelTypes === null || filters.channelTypes.includes(t)}
                onChange={(e) => {
                  const current = filters.channelTypes ?? channelTypes;
                  const next = e.target.checked
                    ? [...current, t]
                    : current.filter((x) => x !== t);
                  onFiltersChange({
                    ...filters,
                    channelTypes: next.length === channelTypes.length ? null : next,
                  });
                }}
              >
                {t}
              </Checkbox>
            </MenuItem>
          ))}
        </MenuList>
      </Menu>
    </HStack>
  );
};
```

- [ ] **Step 5: Wire into DataflowDetailPage**

`filters` state, `decorations = React.useMemo(() => decorateGraph(deriveVisibleGraph(structure, collapsed), filters, heatColor), ...)` where `heatColor = (t) => interpolateYlOrRd(0.15 + 0.85 * t)` from `d3-scale-chromatic` (existing dep). Heat legend: small gradient `Box` + min/max labels next to the heatmap select.

Centering needs React Flow context, so `DataflowGraphView` gains a `centerRef` prop filled by a helper rendered inside `<ReactFlow>`:

```tsx
// DataflowGraphViewProps gains:
//   centerRef?: React.MutableRefObject<((id: string) => void) | null>;

const CenterHelper = ({
  centerRef,
}: {
  centerRef: React.MutableRefObject<((id: string) => void) | null>;
}) => {
  const reactFlow = useReactFlow();
  React.useEffect(() => {
    centerRef.current = (id: string) => {
      const internal = reactFlow.getInternalNode(id);
      if (!internal) return;
      const { x, y } = internal.internals.positionAbsolute;
      const width = internal.measured?.width ?? 0;
      const height = internal.measured?.height ?? 0;
      reactFlow.setCenter(x + width / 2, y + height / 2, { zoom: 1, duration: 300 });
    };
    return () => {
      centerRef.current = null;
    };
  }, [reactFlow, centerRef]);
  return null;
};
// In DataflowGraphView's JSX, inside <ReactFlow>:
//   {centerRef && <CenterHelper centerRef={centerRef} />}
```

Search jump wiring in `DataflowDetailPage`:

```tsx
const centerRef = React.useRef<((id: string) => void) | null>(null);
const [matchIndex, setMatchIndex] = React.useState(0);
// New search: expand ancestors of matches once, reset the cursor.
React.useEffect(() => {
  setMatchIndex(0);
  if (filters.search && data) {
    setCollapsed((c) => (c ? expandForSearch(data.structure, c, filters.search) : c));
  }
}, [filters.search]); // eslint-disable-line react-hooks/exhaustive-deps
const onJump = React.useCallback(
  (delta: 1 | -1) => {
    const matches = decorations.searchMatches;
    if (matches.length === 0) return;
    const next = (matchIndex + delta + matches.length) % matches.length;
    setMatchIndex(next);
    centerRef.current?.(matches[next]);
  },
  [decorations.searchMatches, matchIndex],
);
```

- [ ] **Step 6: Typecheck, lint, tests, manual check, commit**

```bash
yarn test --run src/platform/dataflows/ && yarn typecheck && yarn lint
git add console/src
git commit -m "console: dataflow visualizer filters and toolbar"
```

### Task 14: LIR panel

**Files:**
- Create: `console/src/platform/dataflows/LirPanel.tsx`
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Modify: `console/src/platform/dataflows/dataflowGraph.ts` (one pure helper + test)

**Interfaces:**
- Produces: `lirIndex(structure: DataflowStructure): Map<string, { info: LirInfo; memberIds: NodeId[] }>` keyed by `${exportId}/${lirId}` (test: fixture yields one entry with members `[5,1]`, `[5,1,1]` per span 11..13 covering ids 11 and 12).

- [ ] **Step 1: Failing test + helper**

Test (append to `dataflowGraph.test.ts`):

```typescript
import { lirIndex } from "./dataflowGraph";

describe("lirIndex", () => {
  it("buckets member nodes by export/lir id", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    const index = lirIndex(s);
    expect([...index.keys()]).toEqual(["u42/1"]);
    // span 11..13 covers operator ids 11 ([5,1]) and 12 ([5,1,1])
    expect(index.get("u42/1")!.memberIds.sort()).toEqual(
      [nodeIdOf([5, 1]), nodeIdOf([5, 1, 1])].sort(),
    );
  });
});
```

Implementation:

```typescript
export function lirIndex(
  structure: DataflowStructure,
): Map<string, { info: LirInfo; memberIds: NodeId[] }> {
  const index = new Map<string, { info: LirInfo; memberIds: NodeId[] }>();
  for (const node of structure.nodes.values()) {
    for (const info of node.lir) {
      const key = `${info.exportId}/${info.lirId}`;
      const entry = index.get(key) ?? { info, memberIds: [] };
      entry.memberIds.push(node.id);
      index.set(key, entry);
    }
  }
  return index;
}
```

- [ ] **Step 2: Panel**

```tsx
export const LirPanel = ({
  structure,
  onHighlight,
}: {
  structure: DataflowStructure;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
}) => {
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const byExport = React.useMemo(() => {
    const groups = new Map<string, { key: string; info: LirInfo; memberIds: NodeId[] }[]>();
    for (const [key, entry] of index) {
      const list = groups.get(entry.info.exportId) ?? [];
      list.push({ key, ...entry });
      groups.set(entry.info.exportId, list);
    }
    return groups;
  }, [index]);
  if (index.size === 0) return null;
  return (
    <Box width="280px" borderRightWidth="1px" p={3} overflowY="auto" flexShrink={0}>
      {[...byExport.entries()].map(([exportId, entries]) => (
        <Box key={exportId} mb={3}>
          <Text fontSize="xs" color="gray.500">
            {exportId}
          </Text>
          {entries.map((e) => (
            <Text
              key={e.key}
              fontSize="xs"
              cursor="pointer"
              _hover={{ background: "gray.100" }}
              onMouseEnter={() => onHighlight(new Set(e.memberIds))}
              onMouseLeave={() => onHighlight(null)}
            >
              LIR {e.info.lirId}: {e.info.operator}
            </Text>
          ))}
        </Box>
      ))}
    </Box>
  );
};
```

- [ ] **Step 3: Wire** highlight set in `DataflowDetailPage`: when non-null, everything not in the set joins `decorations.dimmedNodeIds` (merge before passing to the view). Note collapsed ancestors: map each member id through the current collapse state via `representative`-style logic already available (`deriveVisibleGraph` output ids), simplest: dim nothing that is not currently visible, i.e. intersect with visible node ids.

- [ ] **Step 4: Typecheck, lint, manual check (hover LIR row highlights members), commit**

```bash
git add console/src
git commit -m "console: LIR operator panel with member highlighting"
```

---

## Phase 4: hardening and cleanup

### Task 15: Error and empty states + CNS-109 regression test

**Files:**
- Modify: `console/src/platform/dataflows/DataflowDetailPage.tsx`
- Test: `console/src/platform/dataflows/DataflowDetailPage.test.tsx`

- [ ] **Step 1: Empty/gone states**

In `DataflowDetailPage`: when `data` loads and `structure.nodes.size <= 1` (root only or empty), render `Text` "This dataflow no longer exists on this replica." with a `Link` back to `..` (the list). Timeout errors already surface through `error` → `ErrorBox`; add a "Retry" `Button` calling `refetch` under the `ErrorBox`.

- [ ] **Step 2: Write the CNS-109 regression component test**

`DataflowDetailPage.test.tsx`: `mockReactFlow()` + the `useElkLayout` mock from Task 6, msw handler that fails for `cluster_replica === "r2"` and returns a small successful structure otherwise (reuse the row shapes Task 5's test settled on). Setup:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { ErrorCode } from "~/api/materialize/types";
import server from "~/api/mocks/server";
import { getStore } from "~/jotai";
import { allClusters } from "~/store/allClusters";
import { mockReactFlow } from "~/test/mockReactFlow";
import { mockSubscribeState } from "~/test/mockSubscribe";
import { renderComponent } from "~/test/utils";

import DataflowDetailPage from "./DataflowDetailPage";

mockReactFlow();
// plus the useElkLayout vi.mock block from Task 6

const CLUSTER_ID = "u5";
const FAILING_REPLICA = "r2";
const testCluster = {
  id: CLUSTER_ID,
  name: "test_cluster",
  replicas: [{ name: "r1" }, { name: FAILING_REPLICA }],
} as Cluster;

const renderVisualizer = () =>
  renderComponent(
    <Routes>
      <Route
        path="/clusters/:clusterId/:clusterName/dataflows/:dataflowId"
        element={<DataflowDetailPage />}
      />
    </Routes>,
    { initialRouterEntries: [`/clusters/${CLUSTER_ID}/test_cluster/dataflows/7`] },
  );

const okResult = { desc: { columns: [] }, rows: [] };
const operatorsResult = {
  desc: {
    columns: [
      { name: "id" }, { name: "address" }, { name: "name" },
      { name: "arrangementRecords" }, { name: "arrangementSize" }, { name: "elapsedNs" },
    ],
  },
  rows: [
    ["10", ["7"], "Dataflow", "0", "0", "0"],
    ["11", ["7", "1"], "Map", "0", "0", "1"],
  ],
};
const handler = http.post("*/api/sql", async ({ request }) => {
  const options = JSON.parse(
    new URL(request.url).searchParams.get("options") ?? "{}",
  );
  if (options.cluster_replica === FAILING_REPLICA) {
    return HttpResponse.json({
      results: [
        { error: { message: "no such replica", code: ErrorCode.INTERNAL_ERROR } },
      ],
    });
  }
  return HttpResponse.json({ results: [operatorsResult, okResult, okResult] });
});

beforeEach(() => {
  server.use(handler);
  getStore().set(allClusters, mockSubscribeState({ data: [testCluster] }));
});
```

If `renderComponent`'s option name differs, match `console/src/test/utils.tsx`. Assertions:

```tsx
it("shows an error instead of the stale graph when a refetch fails", async () => {
  renderVisualizer();
  const replicaSelect = await screen.findByRole("combobox");
  // graph rendered from r1 data first
  expect(await screen.findByTestId("react-flow")).toBeVisible();
  await userEvent.selectOptions(replicaSelect, "r2");
  expect(
    await screen.findByText("There was an error visualizing your dataflow"),
  ).toBeVisible();
  expect(screen.queryByTestId("react-flow")).toBeNull();
});
```

- [ ] **Step 3: Run test, verify pass** (the Task 5 contract makes it pass; if it fails, the bug is real, fix the hook not the test).

Run: `yarn test --run src/platform/dataflows/DataflowDetailPage.test.tsx`

- [ ] **Step 4: Commit**

```bash
git add console/src
git commit -m "console: dataflow visualizer error states and CNS-109 regression test"
```

### Task 16: Delete the old visualizer and d3-graphviz

**Files:**
- Delete: `console/src/platform/clusters/DataflowVisualizer.tsx`
- Delete: `console/src/api/materialize/useDataflowStructure.ts`
- Modify: `console/package.json`

- [ ] **Step 1: Verify no remaining references**

Run: `grep -rn "DataflowVisualizer\|useDataflowStructure\|d3-graphviz" console/src console/e2e-tests`
Expected: no hits outside the two files being deleted (Task 10 removed the route + import). If hits remain, fix them first.

- [ ] **Step 2: Delete**

```bash
git rm console/src/platform/clusters/DataflowVisualizer.tsx console/src/api/materialize/useDataflowStructure.ts
cd console && yarn remove d3-graphviz
```

`d3` stays (used elsewhere). Confirm `@hpcc-js/wasm` left the lockfile: `grep hpcc console/yarn.lock` → no hits.

- [ ] **Step 3: Full sweep**

Run: `yarn test --run && yarn typecheck && yarn lint && yarn build:local`
Expected: all PASS, build succeeds with no WASM chunk for graphviz.

- [ ] **Step 4: Commit**

```bash
git add console/package.json console/yarn.lock
git commit -m "console: delete graphviz dataflow visualizer and d3-graphviz dep"
```

---

## Self-review notes

* Spec coverage: surface (Tasks 7, 9, 10), data layer + CNS-109 (5, 15), graph model/collapse/LIR (2, 3, 14), rendering/layout/perf guard (4, 6), filters (13), refresh (11), errors (15), deletion/CNS-108-by-construction (6, 16), MVP gate (8). Review items: `Channel` defined (Task 2), `lir` widened to array with test (Task 2).
* Detail panel is spec section "Filters and interactions" (Task 12).
* Type names match across tasks: `VisibleNode`, `VisibleGraph`, `Positions`, `GraphDecorations` (defined once in Task 13, consumed by Task 6 via optional prop declared there with matching shape).
* Known judgment calls delegated to implementers: exact msw row shapes (Task 5 step 1 note), `useFlags` import source (copy from `SimpleObjectDetailRoutes.tsx`), absolute cluster link shape (verify against `ClustersList.tsx` in Task 10).
