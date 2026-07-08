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

export function formatElapsedNs(ns: bigint): string {
  return `${Math.round(Number(ns) / 1e9)}s`;
}

export interface NodeStats {
  arrangementRecords: bigint;
  arrangementSize: bigint;
  elapsedNs: bigint;
}

// How unevenly work for this node is spread across workers: worst worker's
// value over the average, matching EXPLAIN ANALYZE ... WITH SKEW. Avg is
// over workers that show up at all for this node, not the full replica
// worker count. A worker with zero rows anywhere for a node is invisible to
// both, same convention. 1 means perfectly even, higher means more skewed,
// 0 means no data.
export interface SkewStats {
  cpuSkew: number;
  memorySkew: number;
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
  ownSkew: SkewStats;
  transitiveSkew: SkewStats; // recomputed from the subtree's summed per-worker vectors, not from children's own skew (skew doesn't sum)
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

// A boundary crossing this view can't show directly (the far side isn't part
// of it): kept so a port can still be jumped to, even fanning out to several
// peers (one output can feed multiple inputs).
export interface PortPeer {
  address: Address;
  label: string;
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
  // The exact port this crossing lands on from inside the peer, so jumping
  // can drill straight into it rather than parking outside as an unlabeled
  // box. Null when the peer is a leaf (nothing to drill into, so the peer
  // itself is the target).
  peerPortId: NodeId | null;
}

export interface VisibleNode {
  id: string;
  kind: "operator" | "region" | "port";
  label: string;
  stats: NodeStats | null;
  transitive: NodeStats | null;
  transitiveSkew: SkewStats | null;
  childCount: number;
  lir: LirInfo[];
  address: Address | null;
  // null for synthetic port nodes, which have no operator row of their own.
  operatorId: bigint | null;
  // Non-empty only for ports: the out-of-view side of every crossing that
  // resolved to this port and wasn't already reachable some other way.
  peers: PortPeer[];
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
export interface PerWorkerStatRow {
  id: bigint | number | string;
  workerId: bigint | number | string;
  elapsedNs: bigint | number | string | null;
  arrangementSize: bigint | number | string | null;
}

const toBigInt = (v: bigint | number | string | null | undefined): bigint =>
  v == null ? 0n : BigInt(v);

// workerId -> value. A worker absent from a node's vector never scheduled or
// arranged anything for it (no row at all, per the introspection tables'
// convention), which the caller treats as "doesn't count towards average"
// rather than "counts as zero" (matching EXPLAIN ANALYZE ... WITH SKEW).
type WorkerVector = Map<number, bigint>;

function mergeWorkerVectors(a: WorkerVector, b: WorkerVector): WorkerVector {
  if (b.size === 0) return a;
  const merged = new Map(a);
  for (const [workerId, v] of b) {
    merged.set(workerId, (merged.get(workerId) ?? 0n) + v);
  }
  return merged;
}

function skewRatio(vector: WorkerVector): number {
  if (vector.size === 0) return 0;
  let sum = 0n;
  let max = 0n;
  for (const v of vector.values()) {
    sum += v;
    if (v > max) max = v;
  }
  const avg = Number(sum) / vector.size;
  return avg === 0 ? 0 : Number(max) / avg;
}

export function buildDataflowStructure(
  operators: OperatorRow[],
  channels: ChannelRow[],
  lirSpans: LirSpanRow[],
  perWorkerStats: PerWorkerStatRow[] = [],
): DataflowStructure {
  // A dropped or transient dataflow that has already gone away returns zero
  // operator rows, a valid query result rather than an error. Represent it
  // as a single-node placeholder structure instead of throwing below, so it
  // renders through the same nodes.size <= 1 "no longer exists" branch as
  // any other empty dataflow.
  if (operators.length === 0) {
    const placeholderId = nodeIdOf([]);
    return {
      nodes: new Map([
        [
          placeholderId,
          {
            id: placeholderId,
            operatorId: 0n,
            address: [],
            name: "",
            parent: null,
            children: [],
            own: { arrangementRecords: 0n, arrangementSize: 0n, elapsedNs: 0n },
            transitive: {
              arrangementRecords: 0n,
              arrangementSize: 0n,
              elapsedNs: 0n,
            },
            ownSkew: { cpuSkew: 0, memorySkew: 0 },
            transitiveSkew: { cpuSkew: 0, memorySkew: 0 },
            lir: [],
          },
        ],
      ]),
      root: placeholderId,
      channels: [],
    };
  }
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
  const nodeIdByOperatorId = new Map<string, NodeId>();
  for (const row of operators) {
    const address = row.address.map(Number);
    const id = nodeIdOf(address);
    const opId = toBigInt(row.id);
    const own: NodeStats = {
      arrangementRecords: toBigInt(row.arrangementRecords),
      arrangementSize: toBigInt(row.arrangementSize),
      elapsedNs: toBigInt(row.elapsedNs),
    };
    nodeIdByOperatorId.set(opId.toString(), id);
    nodes.set(id, {
      id,
      operatorId: opId,
      address,
      name: row.name,
      parent: address.length > 1 ? nodeIdOf(address.slice(0, -1)) : null,
      children: [],
      own,
      transitive: own, // replaced below
      ownSkew: { cpuSkew: 0, memorySkew: 0 }, // replaced below
      transitiveSkew: { cpuSkew: 0, memorySkew: 0 }, // replaced below
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

  const ownCpuByNode = new Map<NodeId, WorkerVector>();
  const ownMemoryByNode = new Map<NodeId, WorkerVector>();
  for (const row of perWorkerStats) {
    const id = nodeIdByOperatorId.get(toBigInt(row.id).toString());
    if (!id) continue; // operator outside this dataflow's own address tree
    const workerId = Number(row.workerId);
    if (row.elapsedNs != null) {
      const vec = ownCpuByNode.get(id) ?? new Map();
      vec.set(workerId, (vec.get(workerId) ?? 0n) + toBigInt(row.elapsedNs));
      ownCpuByNode.set(id, vec);
    }
    if (row.arrangementSize != null) {
      const vec = ownMemoryByNode.get(id) ?? new Map();
      vec.set(
        workerId,
        (vec.get(workerId) ?? 0n) + toBigInt(row.arrangementSize),
      );
      ownMemoryByNode.set(id, vec);
    }
  }

  const fillTransitive = (
    id: NodeId,
  ): { stats: NodeStats; cpuVec: WorkerVector; memoryVec: WorkerVector } => {
    const node = nodes.get(id)!;
    const ownCpuVec = ownCpuByNode.get(id) ?? new Map();
    const ownMemoryVec = ownMemoryByNode.get(id) ?? new Map();
    node.ownSkew = {
      cpuSkew: skewRatio(ownCpuVec),
      memorySkew: skewRatio(ownMemoryVec),
    };
    const t = { ...node.own };
    let cpuVec = ownCpuVec;
    let memoryVec = ownMemoryVec;
    for (const c of node.children) {
      const child = fillTransitive(c);
      t.arrangementRecords += child.stats.arrangementRecords;
      t.arrangementSize += child.stats.arrangementSize;
      t.elapsedNs += child.stats.elapsedNs;
      cpuVec = mergeWorkerVectors(cpuVec, child.cpuVec);
      memoryVec = mergeWorkerVectors(memoryVec, child.memoryVec);
    }
    node.transitive = t;
    node.transitiveSkew = {
      cpuSkew: skewRatio(cpuVec),
      memorySkew: skewRatio(memoryVec),
    };
    return { stats: t, cpuVec, memoryVec };
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

// A view shows exactly one scope's direct children. Anything deeper is
// rolled up into its child's box, addressed at that box's depth (viewRoot's
// depth + 1). representativeInView is the general form of that projection,
// exported for translating arbitrary structure addresses (search matches,
// LIR members) into the box that would represent them in a given view, or
// null if the address isn't inside viewRootAddress's subtree at all.
export function representativeInView(
  address: Address,
  viewRootAddress: Address,
): NodeId | null {
  if (
    address.length > viewRootAddress.length &&
    viewRootAddress.every((v, i) => address[i] === v)
  ) {
    return nodeIdOf(address.slice(0, viewRootAddress.length + 1));
  }
  return null;
}

function representative(address: Address, viewRootAddress: Address): NodeId {
  return representativeInView(address, viewRootAddress) ?? nodeIdOf(address);
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
//
// A port only ever renders for viewRoot's own boundary: every other scope in
// a view is shown as a collapsed box (never "expanded"), so any crossing at
// its boundary rolls up into that box instead, exactly like a normal channel
// endpoint landing inside it would.
function endpointId(
  structure: DataflowStructure,
  address: Address,
  port: number,
  side: "from" | "to",
  viewRoot: NodeId,
  viewRootAddress: Address,
): {
  id: string;
  port?: { scope: NodeId; direction: "input" | "output"; port: number };
} {
  if (address[address.length - 1] === 0) {
    const scope = address.slice(0, -1);
    const scopeId = nodeIdOf(scope);
    const rep =
      scope.length === 0 ? scopeId : representative(scope, viewRootAddress);
    if (rep !== scopeId || scopeId !== viewRoot) return { id: rep };
    const direction = side === "from" ? "input" : "output";
    return {
      id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
      port: { scope: scopeId, direction, port },
    };
  }
  const scopeId = nodeIdOf(address);
  const scopeNode = structure.nodes.get(scopeId);
  if (scopeNode && scopeNode.children.length > 0) {
    const rep = representative(address, viewRootAddress);
    if (rep === scopeId && scopeId === viewRoot) {
      const direction = side === "to" ? "input" : "output";
      return {
        id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
        port: { scope: scopeId, direction, port },
      };
    }
    return { id: rep };
  }
  return { id: representative(address, viewRootAddress) };
}

// Renders exactly one scope's direct children, each either a leaf operator or
// a box summarizing a whole nested subtree (never expanded in place).
// Double-clicking a box navigates to a new view rooted there instead.
export function deriveVisibleGraph(
  structure: DataflowStructure,
  viewRoot: NodeId,
): VisibleGraph {
  const viewRootNode = structure.nodes.get(viewRoot)!;
  const viewRootAddress = viewRootNode.address;
  const nodes: VisibleNode[] = viewRootNode.children.map((id) => {
    const node = structure.nodes.get(id)!;
    const kind = node.children.length === 0 ? "operator" : "region";
    return {
      id,
      kind,
      label: node.name,
      stats: kind === "region" ? node.transitive : node.own,
      transitive: node.transitive,
      transitiveSkew: node.transitiveSkew,
      childCount: node.children.length,
      lir: node.lir,
      address: node.address,
      operatorId: node.operatorId,
      peers: [],
    };
  });
  const visibleIds = new Set(nodes.map((n) => n.id));

  const edgesById = new Map<string, VisibleEdge & { typeSet: Set<string> }>();
  const ports = new Map<string, VisibleNode>();
  const portId = (p: {
    scope: NodeId;
    direction: "input" | "output";
    port: number;
  }) => `${p.scope}:${p.direction === "input" ? "in" : "out"}:${p.port}`;
  // Records the out-of-view side of a crossing on its port, so a port that
  // can't show its peer directly can still be jumped to. One output can feed
  // several inputs (and vice versa), so this can add up across channels
  // rather than replace. The same peer address showing up twice (e.g. two
  // channel rows for the same logical pair) just accumulates its stats.
  const addPeer = (
    p: { scope: NodeId; direction: "input" | "output"; port: number },
    peerAddress: Address,
    peerPort: number,
    peerSide: "from" | "to",
    messagesSent: bigint,
    batchesSent: bigint,
    channelType: string | null,
  ) => {
    const peerId = nodeIdOf(peerAddress);
    if (!structure.nodes.has(peerId)) return; // elided scope, nothing to jump to
    const port = ports.get(portId(p))!;
    const existing = port.peers.find(
      (peer) => nodeIdOf(peer.address) === peerId,
    );
    if (existing) {
      existing.messagesSent += messagesSent;
      existing.batchesSent += batchesSent;
      if (channelType && !existing.channelTypes.includes(channelType)) {
        existing.channelTypes = [...existing.channelTypes, channelType].sort();
      }
    } else {
      // Re-resolve this same crossing as if viewRoot were the peer itself,
      // to find the exact port a jump should land on inside it. A leaf peer
      // has nothing to drill into, so this naturally comes back port-less.
      const fromPeer = endpointId(
        structure,
        peerAddress,
        peerPort,
        peerSide,
        peerId,
        peerAddress,
      );
      port.peers.push({
        address: peerAddress,
        label: structure.nodes.get(peerId)!.name,
        messagesSent,
        batchesSent,
        channelTypes: channelType ? [channelType] : [],
        peerPortId: fromPeer.port ? fromPeer.id : null,
      });
    }
  };
  for (const ch of structure.channels) {
    const from = endpointId(
      structure,
      ch.fromAddress,
      ch.fromPort,
      "from",
      viewRoot,
      viewRootAddress,
    );
    const to = endpointId(
      structure,
      ch.toAddress,
      ch.toPort,
      "to",
      viewRoot,
      viewRootAddress,
    );
    // A port node is registered whenever either side resolves to one, even
    // if the edge itself is about to be dropped: viewRoot's own boundary can
    // have traffic to a sibling entirely outside this view (nothing on the
    // far side to draw an edge to), and the port stub is the only visible
    // trace that the crossing exists at all.
    for (const p of [from.port, to.port]) {
      if (p && !ports.has(portId(p))) {
        ports.set(portId(p), {
          id: portId(p),
          kind: "port",
          label: `${p.direction} ${p.port}`,
          stats: null,
          transitive: null,
          transitiveSkew: null,
          childCount: 0,
          lir: [],
          address: null,
          operatorId: null,
          peers: [],
        });
      }
    }
    // The far side of a port crossing is worth recording as a jump target
    // only when this view doesn't already show it some other way (e.g. the
    // inner half of a boundary pairing lands on a node already visible here,
    // which needs no jump link since there's already a drawn edge to it).
    if (from.port && !to.port && !visibleIds.has(to.id)) {
      addPeer(
        from.port,
        ch.toAddress,
        ch.toPort,
        "to",
        ch.messagesSent,
        ch.batchesSent,
        ch.channelType,
      );
    }
    if (to.port && !from.port && !visibleIds.has(from.id)) {
      addPeer(
        to.port,
        ch.fromAddress,
        ch.fromPort,
        "from",
        ch.messagesSent,
        ch.batchesSent,
        ch.channelType,
      );
    }
    // A non-port endpoint id must name one of this view's own boxes. Real
    // dataflows can log channels touching an address with no corresponding
    // operator row (e.g. an elided scope), and any address outside viewRoot's
    // subtree resolves to an id this view never emits either; drop the
    // edge (but keep any port registered above) in both cases rather than
    // hand elk a dangling reference.
    if (
      (!from.port && !visibleIds.has(from.id)) ||
      (!to.port && !visibleIds.has(to.id))
    ) {
      continue;
    }
    if (from.id === to.id) continue;
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

// The scope to navigate to so that every given address is directly visible,
// each as either itself (if it's already a direct child of the result) or
// the box that rolls it up. Backs off past any address that IS the raw
// common prefix (so every input lands strictly below the result, never
// equal to it) and past any prefix with no operator row of its own (an
// elided scope can't be navigated to, since it never appears in a view).
export function commonAncestorScope(
  structure: DataflowStructure,
  addresses: readonly Address[],
): NodeId {
  let prefix = addresses[0];
  for (const a of addresses.slice(1)) {
    let len = 0;
    while (len < prefix.length && len < a.length && prefix[len] === a[len]) {
      len++;
    }
    prefix = prefix.slice(0, len);
  }
  const shortest = Math.min(...addresses.map((a) => a.length));
  while (prefix.length >= shortest || !structure.nodes.has(nodeIdOf(prefix))) {
    if (prefix.length === 0) return structure.root;
    prefix = prefix.slice(0, -1);
  }
  return nodeIdOf(prefix);
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

export interface Filters {
  search: string;
  hideIdle: boolean;
  heatmap: "off" | "elapsed" | "size" | "cpuSkew" | "memorySkew";
  heatmapThreshold: number; // 0..1 fraction of max
}

export const DEFAULT_FILTERS: Filters = {
  search: "",
  hideIdle: false,
  heatmap: "off",
  heatmapThreshold: 0,
};

// decorateGraph returns every field set.
export interface GraphDecorations {
  dimmedNodeIds: Set<string>;
  hiddenNodeIds: Set<string>;
  hiddenEdgeIds: Set<string>;
  nodeColors: Map<string, string>;
  searchMatches: string[]; // visible node ids, document order
}

// Search matches anywhere in the whole dataflow, not just the current view
// (search is structure-wide so it stays useful once a graph is too big to
// show in one screen). searchInfo carries that lookup in, precomputed once
// per search string rather than re-walked per view. A box that doesn't
// itself match but contains a match deeper in its rolled-up subtree is left
// undimmed rather than excluded, so the match's path stays visible without
// forcing a navigation on every keystroke.
export function decorateGraph(
  graph: VisibleGraph,
  filters: Filters,
  heatColor: (t: number) => string,
  searchInfo: SubtreeSearchMatches | null,
): GraphDecorations {
  const d: GraphDecorations = {
    dimmedNodeIds: new Set(),
    hiddenNodeIds: new Set(),
    hiddenEdgeIds: new Set(),
    nodeColors: new Map(),
    searchMatches: [],
  };
  for (const n of graph.nodes) {
    if (searchInfo && n.kind !== "port") {
      if (searchInfo.matches.has(n.id)) d.searchMatches.push(n.id);
      else if (!searchInfo.containsMatch.has(n.id)) d.dimmedNodeIds.add(n.id);
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
  }
  if (filters.heatmap !== "off") {
    const heatmap = filters.heatmap;
    const metric = (n: VisibleNode): number => {
      switch (heatmap) {
        case "elapsed":
          return Number(n.transitive?.elapsedNs ?? 0n);
        case "size":
          return Number(n.transitive?.arrangementSize ?? 0n);
        case "cpuSkew":
          return n.transitiveSkew?.cpuSkew ?? 0;
        case "memorySkew":
          return n.transitiveSkew?.memorySkew ?? 0;
      }
    };
    const candidates = graph.nodes.filter((n) => n.kind !== "port");
    const max = candidates.reduce((m, n) => Math.max(m, metric(n)), 0);
    if (max > 0) {
      for (const n of candidates) {
        const t = metric(n) / max;
        d.nodeColors.set(n.id, heatColor(t));
        if (t < filters.heatmapThreshold) d.dimmedNodeIds.add(n.id);
      }
    }
  }
  return d;
}

export interface SubtreeSearchMatches {
  matches: Set<NodeId>; // nodes whose own name matches
  containsMatch: Set<NodeId>; // matches, plus any ancestor of one
}

// Computed once over the whole structure (not the current view), so a box is
// never dimmed just because the current scope hasn't drilled down to the
// match nested inside it yet.
export function subtreeSearchMatches(
  structure: DataflowStructure,
  search: string,
): SubtreeSearchMatches {
  const needle = search.trim().toLowerCase();
  const matches = new Set<NodeId>();
  const containsMatch = new Set<NodeId>();
  if (!needle) return { matches, containsMatch };
  const visit = (id: NodeId): boolean => {
    const node = structure.nodes.get(id)!;
    let hit = node.name.toLowerCase().includes(needle);
    if (hit) matches.add(id);
    for (const c of node.children) {
      if (visit(c)) hit = true;
    }
    if (hit) containsMatch.add(id);
    return hit;
  };
  for (const id of structure.nodes.get(structure.root)!.children) visit(id);
  return { matches, containsMatch };
}

function addressCompare(a: Address, b: Address): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

// The ordered, structure-wide match list prev/next cycles through: jumping
// to a match may navigate the view (unlike decorateGraph's searchMatches,
// which only ever lists what's already visible in the current scope).
export function allSearchMatches(
  structure: DataflowStructure,
  search: string,
): DataflowNode[] {
  const needle = search.trim().toLowerCase();
  if (!needle) return [];
  return [...structure.nodes.values()]
    .filter(
      (node) =>
        node.id !== structure.root && node.name.toLowerCase().includes(needle),
    )
    .sort((a, b) => addressCompare(a.address, b.address));
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
// ancestor of a lower one. Sorting each level descending by lir id therefore
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
