// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type Address = number[];
export type NodeId = string;
export function nodeIdOf(address: Address): NodeId {
  return JSON.stringify(address);
}

export interface NodeStats {
  arrangementRecords: bigint;
  arrangementSize: bigint;
  elapsedNs: bigint;
}

export interface LirInfo {
  exportId: string;
  lirId: string;
  parentLirId: string | null;
  nesting: number;
  operator: string;
}

export interface DataflowNode {
  id: NodeId;
  operatorId: bigint;
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

export type CollapseState = ReadonlySet<NodeId>;

export interface VisibleNode {
  id: string;
  kind: "operator" | "region" | "collapsedRegion" | "port";
  label: string;
  parent: string | null;
  stats: NodeStats | null;
  transitive: NodeStats | null;
  childCount: number;
  lir: LirInfo[];
  address: Address | null;
  // null for synthetic port nodes, which have no operator row of their own.
  operatorId: bigint | null;
}

export interface VisibleEdge {
  id: string;
  source: string;
  target: string;
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
}

export interface VisibleGraph {
  nodes: VisibleNode[];
  edges: VisibleEdge[];
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
  parentLirId: string | null;
  nesting: number;
  operator: string;
  operatorIdStart: bigint | number | string;
  operatorIdEnd: bigint | number | string;
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
    parentLirId: s.parentLirId,
    nesting: s.nesting,
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
      operatorId: opId,
      address,
      name: row.name,
      parent: address.length > 1 ? nodeIdOf(address.slice(0, -1)) : null,
      children: [],
      own,
      transitive: own, // replaced below
      lir: spans
        .filter((s) => s.start <= opId && opId < s.end)
        .map(({ exportId, lirId, parentLirId, nesting, operator }) => ({
          exportId,
          lirId,
          parentLirId,
          nesting,
          operator,
        })),
    });
  }
  const roots: NodeId[] = [];
  for (const node of nodes.values()) {
    if (node.parent === null) roots.push(node.id);
    else nodes.get(node.parent)?.children.push(node.id);
  }
  if (roots.length !== 1) {
    throw new Error(
      `expected exactly one root operator, found ${roots.length}`,
    );
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

export const MAX_VISIBLE_NODES = 1500;

// Shallowest collapsed ancestor absorbs everything beneath it.
function representative(address: Address, collapsed: CollapseState): NodeId {
  for (let len = 1; len < address.length; len++) {
    const prefix = nodeIdOf(address.slice(0, len));
    if (collapsed.has(prefix)) return prefix;
  }
  return nodeIdOf(address);
}

// A scope boundary crossing is logged as two channels that share a port
// number but use different addressing for each half:
//   - the outer half (external <-> scope) addresses the scope by its own
//     operator address, e.g. an external producer's channel row targets
//     address [5,2] directly, the same address BuildRegion's own operator
//     row uses;
//   - the inner half (scope <-> child) addresses the scope's internal
//     fan-out point one level deeper, e.g. [5,2,0].
// Both halves are mapped to the same synthetic port id, keyed by (scope,
// direction, port number), so they visually chain through one port node
// instead of the outer half landing on the region's own node (indistinguishable
// from every other port sharing that scope) or the inner half dangling.
function endpointId(
  structure: DataflowStructure,
  address: Address,
  port: number,
  side: "from" | "to",
  collapsed: CollapseState,
): {
  id: string;
  port?: { scope: NodeId; direction: "input" | "output"; port: number };
} {
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
  const scopeId = nodeIdOf(address);
  const scopeNode = structure.nodes.get(scopeId);
  if (scopeNode && scopeNode.children.length > 0) {
    const rep = representative(address, collapsed);
    if (rep === scopeId && !collapsed.has(scopeId)) {
      const direction = side === "to" ? "input" : "output";
      return {
        id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
        port: { scope: scopeId, direction, port },
      };
    }
    return { id: rep };
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
      node.children.length === 0
        ? "operator"
        : isCollapsed
          ? "collapsedRegion"
          : "region";
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
      operatorId: node.operatorId,
    });
    if (!isCollapsed) for (const c of node.children) emit(c, id);
  };
  for (const c of structure.nodes.get(structure.root)!.children) emit(c, null);

  const edgesById = new Map<string, VisibleEdge & { typeSet: Set<string> }>();
  const ports = new Map<string, VisibleNode>();
  for (const ch of structure.channels) {
    const from = endpointId(
      structure,
      ch.fromAddress,
      ch.fromPort,
      "from",
      collapsed,
    );
    const to = endpointId(structure, ch.toAddress, ch.toPort, "to", collapsed);
    // A non-port endpoint id names an operator or region address, which must
    // exist in the structure. Real dataflows can log channels touching an
    // address with no corresponding operator row (e.g. an elided scope), so
    // drop the channel rather than hand elk a reference to a node that will
    // never be emitted.
    if (
      (!from.port && !structure.nodes.has(from.id)) ||
      (!to.port && !structure.nodes.has(to.id))
    ) {
      continue;
    }
    if (from.id === to.id) continue;
    for (const p of [from.port, to.port]) {
      if (
        p &&
        !ports.has(
          `${p.scope}:${p.direction === "input" ? "in" : "out"}:${p.port}`,
        )
      ) {
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
          operatorId: null,
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

// Hiding a run of idle operators would otherwise sever every path through
// them, splitting the graph into disconnected islands even though a real
// (idle) path exists. Splices a pass-through edge across each hidden run,
// so hiding idle nodes never removes connectivity, only the boxes in
// between. The spliced edge sums messages/batches/types along the run,
// which is almost always 0/0 (that is why the run was hidden) and so
// renders with the same dashed "idle" styling as an ordinary quiet edge.
//
// Cycles (timely feedback loops) are real and must not hang this: `path`
// tracks nodes on the current walk, and re-entering one simply stops that
// branch rather than looping forever.
export function rerouteHiddenNodes(
  graph: VisibleGraph,
  hiddenNodeIds: ReadonlySet<string>,
): VisibleGraph {
  if (hiddenNodeIds.size === 0) return graph;
  const nodes = graph.nodes.filter((n) => !hiddenNodeIds.has(n.id));

  const outAdj = new Map<string, VisibleEdge[]>();
  for (const e of graph.edges) {
    const list = outAdj.get(e.source);
    if (list) list.push(e);
    else outAdj.set(e.source, [e]);
  }

  interface Agg {
    messagesSent: bigint;
    batchesSent: bigint;
    types: Set<string>;
  }

  // Every visible node reachable by walking forward from `from` through only
  // hidden nodes, with edge stats accumulated along the way.
  function reachableVisible(from: string, path: Set<string>): Map<string, Agg> {
    const result = new Map<string, Agg>();
    if (path.has(from)) return result;
    path.add(from);
    for (const e of outAdj.get(from) ?? []) {
      if (!hiddenNodeIds.has(e.target)) {
        merge(result, e.target, e.messagesSent, e.batchesSent, e.channelTypes);
      } else {
        for (const [target, agg] of reachableVisible(e.target, path)) {
          merge(
            result,
            target,
            agg.messagesSent + e.messagesSent,
            agg.batchesSent + e.batchesSent,
            [...agg.types, ...e.channelTypes],
          );
        }
      }
    }
    path.delete(from);
    return result;
  }

  function merge(
    into: Map<string, Agg>,
    target: string,
    messagesSent: bigint,
    batchesSent: bigint,
    types: string[],
  ) {
    const existing = into.get(target);
    if (existing) {
      existing.messagesSent += messagesSent;
      existing.batchesSent += batchesSent;
      for (const t of types) existing.types.add(t);
    } else {
      into.set(target, { messagesSent, batchesSent, types: new Set(types) });
    }
  }

  const edgesById = new Map<string, VisibleEdge>();
  for (const e of graph.edges) {
    if (!hiddenNodeIds.has(e.source) && !hiddenNodeIds.has(e.target)) {
      edgesById.set(e.id, e);
    }
  }
  for (const e of graph.edges) {
    if (hiddenNodeIds.has(e.source) || !hiddenNodeIds.has(e.target)) continue;
    for (const [target, agg] of reachableVisible(
      e.target,
      new Set([e.source]),
    )) {
      const id = `${e.source}=>${target}`;
      const messagesSent = e.messagesSent + agg.messagesSent;
      const batchesSent = e.batchesSent + agg.batchesSent;
      const types = new Set([...e.channelTypes, ...agg.types]);
      const existing = edgesById.get(id);
      if (existing) {
        edgesById.set(id, {
          ...existing,
          messagesSent: existing.messagesSent + messagesSent,
          batchesSent: existing.batchesSent + batchesSent,
          channelTypes: [
            ...new Set([...existing.channelTypes, ...types]),
          ].sort(),
        });
      } else {
        edgesById.set(id, {
          id,
          source: e.source,
          target,
          messagesSent,
          batchesSent,
          channelTypes: [...types].sort(),
        });
      }
    }
  }
  return { nodes, edges: [...edgesById.values()] };
}

export function defaultCollapseState(
  structure: DataflowStructure,
): CollapseState {
  const collapsed = new Set<NodeId>();
  for (const node of structure.nodes.values()) {
    if (node.children.length > 0 && node.id !== structure.root)
      collapsed.add(node.id);
  }
  return collapsed;
}

export function visibleNodeCount(
  structure: DataflowStructure,
  collapsed: CollapseState,
): number {
  return deriveVisibleGraph(structure, collapsed).nodes.length;
}

export interface Filters {
  search: string;
  hideIdle: boolean;
  heatmap: "off" | "elapsed" | "size";
  heatmapThreshold: number; // 0..1 fraction of max
  channelTypes: string[] | null; // null = all
}

export const DEFAULT_FILTERS: Filters = {
  search: "",
  hideIdle: false,
  heatmap: "off",
  heatmapThreshold: 0,
  channelTypes: null,
};

// decorateGraph returns every field set.
export interface GraphDecorations {
  dimmedNodeIds: Set<string>;
  hiddenNodeIds: Set<string>;
  hiddenEdgeIds: Set<string>;
  dimmedEdgeIds: Set<string>;
  nodeColors: Map<string, string>;
  searchMatches: string[]; // visible node ids, document order
}

export function allChannelTypes(structure: DataflowStructure): string[] {
  const types = new Set<string>();
  for (const c of structure.channels)
    if (c.channelType) types.add(c.channelType);
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
        ? (n.transitive?.elapsedNs ?? 0n)
        : (n.transitive?.arrangementSize ?? 0n);
    const candidates = graph.nodes.filter((n) => n.kind !== "port");
    const max = candidates.reduce(
      (m, n) => (metric(n) > m ? metric(n) : m),
      0n,
    );
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

// Search may match inside collapsed regions. Returns a collapse state with
// every ancestor of every matching node expanded (respecting nothing else).
// Expands every ancestor region of the given addresses, so their operators
// are guaranteed visible regardless of current collapse state.
export function expandAncestorsOf(
  collapsed: CollapseState,
  addresses: Iterable<Address>,
): CollapseState {
  const next = new Set(collapsed);
  for (const address of addresses) {
    for (let len = 1; len < address.length; len++) {
      next.delete(nodeIdOf(address.slice(0, len)));
    }
  }
  return next;
}

export function expandForSearch(
  structure: DataflowStructure,
  collapsed: CollapseState,
  search: string,
): CollapseState {
  const needle = search.trim().toLowerCase();
  if (!needle) return collapsed;
  const matches = [...structure.nodes.values()].filter((node) =>
    node.name.toLowerCase().includes(needle),
  );
  return expandAncestorsOf(
    collapsed,
    matches.map((node) => node.address),
  );
}

// Groups operator nodes by the LIR span covering them, keyed
// `${exportId}/${lirId}`. One dataflow can back several exports, so entries
// are not unique per lir id across exports.
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

export interface LirTreeNode {
  key: string;
  info: LirInfo;
  memberIds: NodeId[];
  children: LirTreeNode[];
  summary: NodeStats;
}

// Sums each member's OWN stats (not transitive): every operator belonging to
// a LIR id appears in memberIds exactly once, including a parent LIR id's
// descendants (buildDataflowStructure assigns an operator to every ancestor
// span that contains it), so this matches EXPLAIN ANALYZE's own total_memory/
// total_records/total_elapsed exactly without a separate query: the same
// operator rows are already fetched for the graph itself.
export function lirSummary(
  structure: DataflowStructure,
  memberIds: readonly NodeId[],
): NodeStats {
  const summary: NodeStats = {
    arrangementRecords: 0n,
    arrangementSize: 0n,
    elapsedNs: 0n,
  };
  for (const id of memberIds) {
    const own = structure.nodes.get(id)?.own;
    if (!own) continue;
    summary.arrangementRecords += own.arrangementRecords;
    summary.arrangementSize += own.arrangementSize;
    summary.elapsedNs += own.elapsedNs;
  }
  return summary;
}

// Arranges lirIndex's flat entries into a tree per export, following
// parentLirId. A lir id is assigned once its own build (and so every
// descendant's) completes, so within one export a higher lir id is never an
// ancestor of a lower one; sorting each level descending by lir id therefore
// matches construction order without needing it as an explicit input, the
// same convention EXPLAIN ANALYZE's own tree rendering uses.
export function lirTree(
  structure: DataflowStructure,
  index: ReadonlyMap<string, { info: LirInfo; memberIds: NodeId[] }>,
): Map<string, LirTreeNode[]> {
  const byExport = new Map<string, Map<string, LirTreeNode>>();
  for (const { info, memberIds } of index.values()) {
    const byLirId = byExport.get(info.exportId) ?? new Map();
    byLirId.set(info.lirId, {
      key: `${info.exportId}/${info.lirId}`,
      info,
      memberIds,
      children: [],
      summary: lirSummary(structure, memberIds),
    });
    byExport.set(info.exportId, byLirId);
  }
  const sortDesc = (nodes: LirTreeNode[]) => {
    nodes.sort((a, b) => Number(b.info.lirId) - Number(a.info.lirId));
    for (const n of nodes) sortDesc(n.children);
  };
  const roots = new Map<string, LirTreeNode[]>();
  for (const [exportId, byLirId] of byExport) {
    const exportRoots: LirTreeNode[] = [];
    for (const node of byLirId.values()) {
      const parent = node.info.parentLirId
        ? byLirId.get(node.info.parentLirId)
        : undefined;
      if (parent) parent.children.push(node);
      else exportRoots.push(node);
    }
    sortDesc(exportRoots);
    roots.set(exportId, exportRoots);
  }
  return roots;
}
