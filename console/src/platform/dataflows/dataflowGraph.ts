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

/**
 * Looks up a key expected to be present by construction (e.g. a child id
 * drawn from the same map it indexes into). Throws with the key in the
 * message instead of a bare non-null assertion, so a broken invariant fails
 * loudly at the lookup site rather than as a generic "undefined" downstream.
 */
export function mustGet<K, V>(map: ReadonlyMap<K, V>, key: K): V {
  const value = map.get(key);
  if (value === undefined) {
    throw new Error(`missing expected map entry for key ${String(key)}`);
  }
  return value;
}

export interface NodeStats {
  arrangementRecords: bigint;
  arrangementSize: bigint;
  elapsedNs: bigint;
  // Total times this operator was scheduled (summed over the scheduling
  // duration histogram's buckets). A high count relative to elapsed time
  // means the operator yields often. Each activation does little work, so
  // per-activation overhead (waking the worker, progress tracking)
  // dominates, and raw elapsed time alone doesn't surface this.
  scheduleCount: bigint;
}

/**
 * How unevenly work for this node is spread across workers: worst worker's
 * value over the average, matching EXPLAIN ANALYZE ... WITH SKEW. Avg is
 * over workers that show up at all for this node, not the full replica
 * worker count. A worker with zero rows anywhere for a node is invisible to
 * both, same convention. 1 means perfectly even, higher means more skewed,
 * 0 means no data.
 */
export interface SkewStats {
  cpuSkew: number;
  memorySkew: number;
  scheduleSkew: number;
}

export interface LirInfo {
  exportId: string;
  lirId: string;
  parentLirId: string | null;
  nesting: number;
  operator: string;
}

/**
 * One node of a dataflow's internal address tree: pure introspection data
 * (own/transitive stats and skew), independent of any particular scope's
 * view. `deriveVisibleGraph` projects this into `VisibleNode`s for a given
 * view; this type carries no view-specific computation itself.
 */
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
  // Same ratios as ownSkew, but never suppressed by the magnitude floor.
  // ownSkew exists to seed transitiveSkew's MAX rollup (see its comment for
  // why negligible nodes must not pollute an ancestor's reported skew).
  // ownSkewRaw is this node's actual measured skew, for display when this
  // exact node is selected, where a floor built for ancestor-safety would
  // otherwise misreport a real (if individually low-stakes) imbalance as
  // "no data".
  ownSkewRaw: SkewStats;
  // The worst skew anywhere in the subtree (this node's own, or any
  // descendant's), matching EXPLAIN ANALYZE's own MAX-based rollup. Not a
  // merged-and-then-ratioed per-worker vector: two operators skewed
  // toward different workers (or the same worker in opposite directions)
  // can sum to a nearly-even total even though each is individually bad,
  // which a merge would hide and a max surfaces.
  transitiveSkew: SkewStats;
  lir: LirInfo[]; // one entry per export span covering this operator id
  // Timely's own-elapsed accounting is exclusive self time: an operator's
  // own.elapsedNs already excludes time spent inside any nested/child
  // operator's own activation, even though scheduling a region
  // synchronously runs its children first (verified against live
  // introspection data: a region's own.elapsedNs can come in well below
  // its direct children's own.elapsedNs summed, which an inclusive
  // wall-clock reading never could). Comparing self time to self time (own
  // minus direct children's own, not their transitive) isolates the
  // region's own dispatch overhead: pointstamp/progress tracking,
  // activation logic, everything beyond simply handing off to each direct
  // child once. Equals own.elapsedNs for a leaf (no children to subtract),
  // which isn't a meaningful "overhead" reading on its own.
  overheadNs: bigint;
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

/**
 * A boundary crossing this view can't show directly (the far side isn't part
 * of it): kept so a port can still be jumped to, even fanning out to several
 * peers (one output can feed multiple inputs).
 */
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
  // For a region, the rolled-up subtree total (same as transitive): a
  // quick canvas-level preview, not this node's own activity. See `own`
  // for that.
  stats: NodeStats | null;
  transitive: NodeStats | null;
  // This node's own stats, never rolled up: for a region, its own
  // dispatch/activation cost distinct from anything its children did (see
  // DataflowNode.overheadNs). `stats` collapses this into the subtree
  // total for regions, which is the right quick read on the canvas but
  // hides this node's own contribution entirely, which the detail panel
  // needs to show on its own.
  own: NodeStats | null;
  // This node's own skew, unfiltered by the magnitude floor that guards
  // transitiveSkew's ancestor rollup (see DataflowNode.ownSkewRaw). The
  // real number for this exact node, not a heatmap-safe one.
  ownSkew: SkewStats | null;
  transitiveSkew: SkewStats | null;
  // null for synthetic port nodes, same as transitive/transitiveSkew (no
  // operator row of their own to compute it from).
  overheadNs: bigint | null;
  childCount: number;
  lir: LirInfo[];
  address: Address | null;
  // null for synthetic port nodes, which have no operator row of their own.
  operatorId: bigint | null;
  // Non-empty only for ports: the out-of-view side of every crossing that
  // resolved to this port and wasn't already reachable some other way.
  peers: PortPeer[];
  // Null for operators/regions. For a port, which side of the crossing it
  // sits on: an "input" port's peers are upstream of it (the connected edge
  // drawn in-view is downstream), an "output" port's peers are downstream
  // (the connected edge is upstream).
  direction: "input" | "output" | null;
  // Set on a region whose children this derivation materialized (in-place
  // expansion). Drives RegionNode's expanded rendering and elk's container
  // layout. Absent on a collapsed region and on every non-region node.
  expanded?: boolean;
  // The enclosing expanded scope's id when this node is nested inside one,
  // driving React Flow / elk nesting. Absent for viewRoot's own direct
  // children, which sit at the top level.
  parentId?: NodeId;
}

/**
 * A rendered edge in one scope's view: a real `Channel`'s raw endpoints,
 * resolved to whatever's actually visible at this scope (an exact node, or
 * a collapsed region/port peeled back to it). Several real channels can
 * merge onto one `VisibleEdge` when they share the same visible endpoints.
 */
export interface VisibleEdge {
  id: string;
  source: string;
  target: string;
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
  // When source/target is a collapsed region, which of its own inner ports
  // this edge's real channel(s) land on (Timely always gates a scope
  // crossing through the box's own address, one hop at a time, so it's
  // never any deeper than that). Reuses PortPeer's shape: address is the
  // box's own address (what onJumpToPeer navigates into) and peerPortId the
  // resolved inner port (what it selects there). A merge of several real
  // channels onto one visible edge (same collapse that merges their stats,
  // e.g. two of a region's outputs feeding the same downstream sink) can
  // name more than one. Empty when the endpoint is already exact (a leaf
  // operator or port, or a box with no matching inner port for this hop).
  sourceLandings: PortPeer[];
  targetLandings: PortPeer[];
}

export interface VisibleGraph {
  nodes: VisibleNode[];
  edges: VisibleEdge[];
}

/**
 * SQL row shapes (values arrive as strings or numbers, normalized here)
 */
export interface OperatorRow {
  id: bigint | number | string;
  address: string[];
  name: string;
  arrangementRecords: bigint | number | string | null;
  arrangementSize: bigint | number | string | null;
  elapsedNs: bigint | number | string | null;
  // Optional (unlike the fields above) so fixtures unrelated to schedule
  // counting don't all need updating: real query rows always provide it
  // (see useDataflowGraphData.ts's COALESCE), omitting it just reads as 0
  // via toBigInt's undefined handling, same as an explicit null would.
  scheduleCount?: bigint | number | string | null;
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
  // See OperatorRow.scheduleCount for why this is optional.
  scheduleCount?: bigint | number | string | null;
}

const toBigInt = (v: bigint | number | string | null | undefined): bigint =>
  v == null ? 0n : BigInt(v);

// workerId -> value. A worker absent from a node's vector never scheduled or
// arranged anything for it (no row at all, per the introspection tables'
// convention), which the caller treats as "doesn't count towards average"
// rather than "counts as zero" (matching EXPLAIN ANALYZE ... WITH SKEW).
type WorkerVector = Map<number, bigint>;

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

// A node's own skew is only meaningful when the underlying work is real: an
// operator that ran once for a few nanoseconds while another worker did
// nothing can post an enormous max/avg ratio despite contributing nothing
// to the dataflow's actual cost, and since transitiveSkew rolls up as a
// MAX (see DataflowNode), that single negligible node's inflated ratio
// would otherwise become every ancestor's reported skew all the way to
// the root. A node's own magnitude (elapsed for cpu, arrangement size for
// memory) must be at least this fraction of the busiest single node's own
// magnitude anywhere in the dataflow to count. Below that floor, ownSkew
// reports the same 0 ("no data") sentinel a node with no per-worker rows at
// all would.
const SKEW_MAGNITUDE_FLOOR_FRACTION = 0.01;

function passesMagnitudeFloor(own: bigint, maxOwn: bigint): boolean {
  return (
    maxOwn === 0n ||
    Number(own) >= Number(maxOwn) * SKEW_MAGNITUDE_FLOOR_FRACTION
  );
}

/**
 * Builds the address tree and own/transitive stats from raw introspection
 * rows. `operators` must contain at most one root (an address of length 1);
 * more than one throws. An empty `operators` list (a dropped or transient
 * dataflow) is valid input, not an error: it returns a single-node
 * placeholder structure instead.
 */
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
            own: {
              arrangementRecords: 0n,
              arrangementSize: 0n,
              elapsedNs: 0n,
              scheduleCount: 0n,
            },
            transitive: {
              arrangementRecords: 0n,
              arrangementSize: 0n,
              elapsedNs: 0n,
              scheduleCount: 0n,
            },
            ownSkew: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 },
            ownSkewRaw: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 },
            transitiveSkew: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 },
            lir: [],
            overheadNs: 0n,
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
      scheduleCount: toBigInt(row.scheduleCount),
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
      ownSkew: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 }, // replaced below
      ownSkewRaw: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 }, // replaced below
      transitiveSkew: { cpuSkew: 0, memorySkew: 0, scheduleSkew: 0 }, // replaced below
      lir: spans
        .filter((s) => s.start <= opId && opId < s.end)
        .map(({ exportId, lirId, parentLirId, nesting, operator }) => ({
          exportId,
          lirId,
          parentLirId,
          nesting,
          operator,
        })),
      overheadNs: 0n, // replaced below
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
      const [x, y] = [mustGet(nodes, a).address, mustGet(nodes, b).address];
      return x[x.length - 1] - y[y.length - 1];
    });
  }

  const ownCpuByNode = new Map<NodeId, WorkerVector>();
  const ownMemoryByNode = new Map<NodeId, WorkerVector>();
  const ownScheduleByNode = new Map<NodeId, WorkerVector>();
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
    if (row.scheduleCount != null) {
      const vec = ownScheduleByNode.get(id) ?? new Map();
      vec.set(
        workerId,
        (vec.get(workerId) ?? 0n) + toBigInt(row.scheduleCount),
      );
      ownScheduleByNode.set(id, vec);
    }
  }

  let maxOwnElapsedNs = 0n;
  let maxOwnArrangementSize = 0n;
  let maxOwnScheduleCount = 0n;
  for (const node of nodes.values()) {
    if (node.own.elapsedNs > maxOwnElapsedNs) {
      maxOwnElapsedNs = node.own.elapsedNs;
    }
    if (node.own.arrangementSize > maxOwnArrangementSize) {
      maxOwnArrangementSize = node.own.arrangementSize;
    }
    if (node.own.scheduleCount > maxOwnScheduleCount) {
      maxOwnScheduleCount = node.own.scheduleCount;
    }
  }

  const fillTransitive = (id: NodeId): void => {
    const node = mustGet(nodes, id);
    const ownCpuVec = ownCpuByNode.get(id) ?? new Map();
    const ownMemoryVec = ownMemoryByNode.get(id) ?? new Map();
    const ownScheduleVec = ownScheduleByNode.get(id) ?? new Map();
    node.ownSkew = {
      cpuSkew: passesMagnitudeFloor(node.own.elapsedNs, maxOwnElapsedNs)
        ? skewRatio(ownCpuVec)
        : 0,
      memorySkew: passesMagnitudeFloor(
        node.own.arrangementSize,
        maxOwnArrangementSize,
      )
        ? skewRatio(ownMemoryVec)
        : 0,
      scheduleSkew: passesMagnitudeFloor(
        node.own.scheduleCount,
        maxOwnScheduleCount,
      )
        ? skewRatio(ownScheduleVec)
        : 0,
    };
    node.ownSkewRaw = {
      cpuSkew: skewRatio(ownCpuVec),
      memorySkew: skewRatio(ownMemoryVec),
      scheduleSkew: skewRatio(ownScheduleVec),
    };
    const t = { ...node.own };
    let cpuSkew = node.ownSkew.cpuSkew;
    let memorySkew = node.ownSkew.memorySkew;
    let scheduleSkew = node.ownSkew.scheduleSkew;
    let childrenOwnElapsedNs = 0n;
    for (const c of node.children) {
      fillTransitive(c);
      const child = mustGet(nodes, c);
      t.arrangementRecords += child.transitive.arrangementRecords;
      t.arrangementSize += child.transitive.arrangementSize;
      t.elapsedNs += child.transitive.elapsedNs;
      t.scheduleCount += child.transitive.scheduleCount;
      childrenOwnElapsedNs += child.own.elapsedNs;
      cpuSkew = Math.max(cpuSkew, child.transitiveSkew.cpuSkew);
      memorySkew = Math.max(memorySkew, child.transitiveSkew.memorySkew);
      scheduleSkew = Math.max(scheduleSkew, child.transitiveSkew.scheduleSkew);
    }
    node.transitive = t;
    node.transitiveSkew = { cpuSkew, memorySkew, scheduleSkew };
    node.overheadNs = node.own.elapsedNs - childrenOwnElapsedNs;
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

/**
 * A view shows exactly one scope's direct children. Anything deeper is
 * rolled up into its child's box, addressed at that box's depth (viewRoot's
 * depth + 1). representativeInView is the general form of that projection,
 * exported for translating arbitrary structure addresses (search matches,
 * LIR members) into the box that would represent them in a given view, or
 * null if the address isn't inside viewRootAddress's subtree at all.
 */
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

function addressesEqual(a: Address, b: Address): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

// The box that represents `address` given the set of scopes expanded around
// it: the child one level below the deepest expanded scope that strictly
// contains `address`. With `expanded = {viewRoot}` this is exactly
// representativeInView's viewRootAddress.length + 1 rule. Falls back to the
// address's own id when no expanded scope contains it (addresses outside the
// subtree, which appear in structure.channels).
function representative(address: Address, expanded: Set<NodeId>): NodeId {
  for (let len = address.length - 1; len >= 0; len--) {
    if (expanded.has(nodeIdOf(address.slice(0, len)))) {
      return nodeIdOf(address.slice(0, len + 1));
    }
  }
  return nodeIdOf(address);
}

// A scope boundary crossing is logged as two channels that share a port
// number but use different addressing for each half:
//   - The outer half (external <-> scope) addresses the scope by its own
//     operator address, e.g. an external producer's channel row targets
//     address [5,2] directly, the same address BuildRegion's own operator
//     row uses.
//   - The inner half (scope <-> child) addresses the scope's internal
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
  localRoot: NodeId,
  localRootAddress: Address,
  expanded: Set<NodeId>,
): {
  id: string;
  port?: { scope: NodeId; direction: "input" | "output"; port: number };
} {
  if (address[address.length - 1] === 0) {
    const scope = address.slice(0, -1);
    const scopeId = nodeIdOf(scope);
    const rep = scope.length === 0 ? scopeId : representative(scope, expanded);
    if (rep !== scopeId || !expanded.has(scopeId)) return { id: rep };
    const direction = side === "from" ? "input" : "output";
    return {
      id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
      port: { scope: scopeId, direction, port },
    };
  }
  const scopeId = nodeIdOf(address);
  const scopeNode = structure.nodes.get(scopeId);
  if (scopeNode && scopeNode.children.length > 0) {
    const rep = representative(address, expanded);
    if (rep === scopeId && expanded.has(scopeId)) {
      const direction = side === "to" ? "input" : "output";
      return {
        id: `${scopeId}:${direction === "input" ? "in" : "out"}:${port}`,
        port: { scope: scopeId, direction, port },
      };
    }
    return { id: rep };
  }
  return { id: representative(address, expanded) };
}

/**
 * Renders exactly one scope's direct children, each either a leaf operator or
 * a box summarizing a whole nested subtree (never expanded in place).
 * Double-clicking a box navigates to a new view rooted there instead.
 */
export function deriveVisibleGraph(
  structure: DataflowStructure,
  viewRoot: NodeId,
  expandedScopes: Set<NodeId> = new Set(),
): VisibleGraph {
  const viewRootNode = mustGet(structure.nodes, viewRoot);
  const viewRootAddress = viewRootNode.address;
  const expanded = new Set<NodeId>([viewRoot, ...expandedScopes]);
  const nodes: VisibleNode[] = [];
  const makeNode = (id: NodeId, parentScope: NodeId): VisibleNode => {
    const node = mustGet(structure.nodes, id);
    const kind = node.children.length === 0 ? "operator" : "region";
    return {
      id,
      kind,
      label: node.name,
      stats: kind === "region" ? node.transitive : node.own,
      transitive: node.transitive,
      own: node.own,
      ownSkew: node.ownSkewRaw,
      transitiveSkew: node.transitiveSkew,
      overheadNs: node.overheadNs,
      childCount: node.children.length,
      lir: node.lir,
      address: node.address,
      operatorId: node.operatorId,
      peers: [],
      direction: null,
      expanded: kind === "region" && expandedScopes.has(id),
      // viewRoot's own children stay top-level (no parentId).
      parentId: parentScope === viewRoot ? undefined : parentScope,
    };
  };
  // Emit each scope's children, pushing a container before descending into
  // it, so React Flow's parent-before-child array order holds at any depth.
  const emitChildren = (scope: NodeId) => {
    for (const childId of mustGet(structure.nodes, scope).children) {
      nodes.push(makeNode(childId, scope));
      if (expandedScopes.has(childId)) emitChildren(childId);
    }
  };
  emitChildren(viewRoot);
  const visibleIds = new Set(nodes.map((n) => n.id));

  // sourceLandings/targetLandings are computed separately (see
  // sourceLandingsByEdge/targetLandingsByEdge below) and attached once, in
  // the final map at the bottom, rather than threaded through here.
  const edgesById = new Map<
    string,
    Omit<VisibleEdge, "sourceLandings" | "targetLandings"> & {
      typeSet: Set<string>;
    }
  >();
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
    // A scope's own boundary is logged as a pseudo-vertex [scope, 0], which
    // never gets an operator row of its own (only real children do). A
    // region nested two or more scopes deep has its outer half target that
    // pseudo-vertex directly, one level further up than the region itself.
    // Peel back to the enclosing scope, which does have a row, so the jump
    // still lands somewhere instead of being dropped as if elided.
    let resolvedAddress = peerAddress;
    while (
      resolvedAddress.length > 0 &&
      resolvedAddress[resolvedAddress.length - 1] === 0 &&
      !structure.nodes.has(nodeIdOf(resolvedAddress))
    ) {
      resolvedAddress = resolvedAddress.slice(0, -1);
    }
    const peerId = nodeIdOf(resolvedAddress);
    if (!structure.nodes.has(peerId)) return; // elided scope, nothing to jump to
    const port = mustGet(ports, portId(p));
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
        resolvedAddress,
        new Set([peerId]),
      );
      port.peers.push({
        address: resolvedAddress,
        label: mustGet(structure.nodes, peerId).name,
        messagesSent,
        batchesSent,
        channelTypes: channelType ? [channelType] : [],
        peerPortId: fromPeer.port ? fromPeer.id : null,
      });
    }
  };
  // A channel touching a collapsed region box always does so at exactly the
  // box's own address (Timely gates every scope crossing through a single
  // boundary vertex, one hop at a time. A plain, non-port box reference is
  // therefore never deeper than its own address, unlike addPeer's peers,
  // which land outside viewRoot's subtree entirely). What can still be
  // ambiguous is which of the box's own inner ports a given hop's port
  // number lands on, and a merge of several such hops onto one visible
  // edge (same collapse that merges their stats, e.g. two of a region's
  // outputs feeding the same downstream sink) can name more than one.
  // Recorded per side, keyed by edge id.
  const sourceLandingsByEdge = new Map<string, Map<NodeId, PortPeer>>();
  const targetLandingsByEdge = new Map<string, Map<NodeId, PortPeer>>();
  const addLanding = (
    byEdge: Map<string, Map<NodeId, PortPeer>>,
    edgeId: string,
    boxAddress: Address,
    innerPortId: NodeId,
    label: string,
    messagesSent: bigint,
    batchesSent: bigint,
    channelType: string | null,
  ) => {
    let landings = byEdge.get(edgeId);
    if (!landings) {
      landings = new Map();
      byEdge.set(edgeId, landings);
    }
    const existing = landings.get(innerPortId);
    if (existing) {
      existing.messagesSent += messagesSent;
      existing.batchesSent += batchesSent;
      if (channelType && !existing.channelTypes.includes(channelType)) {
        existing.channelTypes = [...existing.channelTypes, channelType].sort();
      }
    } else {
      landings.set(innerPortId, {
        address: boxAddress,
        label,
        messagesSent,
        batchesSent,
        channelTypes: channelType ? [channelType] : [],
        peerPortId: innerPortId,
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
      expanded,
    );
    const to = endpointId(
      structure,
      ch.toAddress,
      ch.toPort,
      "to",
      viewRoot,
      viewRootAddress,
      expanded,
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
          own: null,
          ownSkew: null,
          transitiveSkew: null,
          overheadNs: null,
          childCount: 0,
          lir: [],
          address: null,
          operatorId: null,
          peers: [],
          direction: p.direction,
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
    // subtree resolves to an id this view never emits either. Drop the
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
    // A landing only adds information when the channel touches the box at
    // exactly its own address (the boundary-crossing pattern endpointId's
    // ends-in-0 pairing relies on): only then does it name one of the box's
    // own inner ports to resolve. Anything else (a leaf collapsing into an
    // ancestor box, an address that isn't this box's boundary at all) has
    // no more precise a target than the box itself, already shown.
    if (!from.port) {
      const box = mustGet(structure.nodes, from.id);
      if (addressesEqual(ch.fromAddress, box.address)) {
        const inner = endpointId(
          structure,
          ch.fromAddress,
          ch.fromPort,
          "from",
          from.id,
          box.address,
          new Set([from.id]),
        );
        if (inner.port) {
          addLanding(
            sourceLandingsByEdge,
            id,
            box.address,
            inner.id,
            `${inner.port.direction} ${inner.port.port}`,
            ch.messagesSent,
            ch.batchesSent,
            ch.channelType,
          );
        }
      }
    }
    if (!to.port) {
      const box = mustGet(structure.nodes, to.id);
      if (addressesEqual(ch.toAddress, box.address)) {
        const inner = endpointId(
          structure,
          ch.toAddress,
          ch.toPort,
          "to",
          to.id,
          box.address,
          new Set([to.id]),
        );
        if (inner.port) {
          addLanding(
            targetLandingsByEdge,
            id,
            box.address,
            inner.id,
            `${inner.port.direction} ${inner.port.port}`,
            ch.messagesSent,
            ch.batchesSent,
            ch.channelType,
          );
        }
      }
    }
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
    sourceLandings: [...(sourceLandingsByEdge.get(e.id)?.values() ?? [])],
    targetLandings: [...(targetLandingsByEdge.get(e.id)?.values() ?? [])],
  }));
  return { nodes: [...nodes, ...ports.values()], edges };
}

/**
 * The scope to navigate to so that every given address is directly visible,
 * each as either itself (if it's already a direct child of the result) or
 * the box that rolls it up. Backs off past any address that IS the raw
 * common prefix (so every input lands strictly below the result, never
 * equal to it) and past any prefix with no operator row of its own (an
 * elided scope can't be navigated to, since it never appears in a view).
 */
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

/**
 * Hiding a run of idle operators would otherwise sever every path through
 * them, splitting the graph into disconnected islands even though a real
 * (idle) path exists. Splices a pass-through edge across each hidden run,
 * so hiding idle nodes never removes connectivity, only the boxes in
 * between. The spliced edge sums messages/batches/types along the run,
 * which is almost always 0/0 (that is why the run was hidden) and so
 * renders with the same dashed "idle" styling as an ordinary quiet edge.
 *
 * Cycles (timely feedback loops) are real and must not hang this: `path`
 * tracks nodes on the current walk, and re-entering one simply stops that
 * branch rather than looping forever.
 */
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
          // A spliced edge crosses one or more hidden nodes: it no longer
          // corresponds to a single real channel, so there's no landing
          // list to carry over.
          sourceLandings: [],
          targetLandings: [],
        });
      }
    }
  }
  return { nodes, edges: [...edgesById.values()] };
}

export interface Filters {
  search: string;
  hideIdle: boolean;
  heatmap:
    | "off"
    | "elapsed"
    | "size"
    | "schedules"
    | "cpuSkew"
    | "memorySkew"
    | "scheduleSkew";
  heatmapThreshold: number; // 0..1 fraction of max
  showLirGroups: boolean;
}

export const DEFAULT_FILTERS: Filters = {
  search: "",
  hideIdle: false,
  heatmap: "off",
  heatmapThreshold: 0,
  showLirGroups: true,
};

/**
 * decorateGraph returns every field set.
 */
export interface GraphDecorations {
  dimmedNodeIds: Set<string>;
  hiddenNodeIds: Set<string>;
  hiddenEdgeIds: Set<string>;
  nodeColors: Map<string, string>;
  searchMatches: string[]; // visible node ids, document order
}

/**
 * Search matches anywhere in the whole dataflow, not just the current view
 * (search is structure-wide so it stays useful once a graph is too big to
 * show in one screen). searchInfo carries that lookup in, precomputed once
 * per search string rather than re-walked per view. A box that doesn't
 * itself match but contains a match deeper in its rolled-up subtree is left
 * undimmed rather than excluded, so the match's path stays visible without
 * forcing a navigation on every keystroke.
 */
export function decorateGraph(
  graph: VisibleGraph,
  filters: Filters,
  heatColor: (t: number) => string,
  searchInfo: SubtreeSearchMatches | null,
  // The replica's total worker count: skewRatio (max-worker-over-average)
  // is mathematically bounded above by it (one worker doing 100% of a
  // fixed amount of work, the rest 0%, is the most skewed that work can
  // ever be split across this many workers). Used only by the cpuSkew/
  // memorySkew/scheduleSkew heatmap modes, as a fixed color-scale ceiling.
  workerCount: number,
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
        case "schedules":
          return Number(n.transitive?.scheduleCount ?? 0n);
        case "cpuSkew":
          return n.transitiveSkew?.cpuSkew ?? 0;
        case "memorySkew":
          return n.transitiveSkew?.memorySkew ?? 0;
        case "scheduleSkew":
          return n.transitiveSkew?.scheduleSkew ?? 0;
      }
    };
    const isSkew =
      heatmap === "cpuSkew" ||
      heatmap === "memorySkew" ||
      heatmap === "scheduleSkew";
    const candidates = graph.nodes.filter((n) => n.kind !== "port");
    if (isSkew) {
      // A skew ratio is bounded above by workerCount (see decorateGraph's
      // own doc comment), and real skew clusters near 1 in practice: a
      // linear [1, workerCount] scale would crush a genuinely bad 2x-3x
      // skew near the cold end on any replica with more than a handful of
      // workers. log2 spreads each doubling of skew evenly across the
      // color range instead, matching the ratio's multiplicative nature
      // (the same reason decibels/Richter use log scales for other
      // ratio-based measurements), and is fixed rather than derived from
      // whatever else happens to be in the current view.
      const domainMax = Math.log2(Math.max(1, workerCount));
      if (domainMax > 0) {
        for (const n of candidates) {
          const t = Math.max(
            0,
            Math.min(1, Math.log2(Math.max(1, metric(n))) / domainMax),
          );
          d.nodeColors.set(n.id, heatColor(t));
          if (t < filters.heatmapThreshold) d.dimmedNodeIds.add(n.id);
        }
      }
    } else {
      const max = candidates.reduce((m, n) => Math.max(m, metric(n)), 0);
      if (max > 0) {
        for (const n of candidates) {
          const t = metric(n) / max;
          d.nodeColors.set(n.id, heatColor(t));
          if (t < filters.heatmapThreshold) d.dimmedNodeIds.add(n.id);
        }
      }
    }
  }
  return d;
}

export interface SubtreeSearchMatches {
  matches: Set<NodeId>; // nodes whose own name matches
  containsMatch: Set<NodeId>; // matches, plus any ancestor of one
}

/**
 * Computed once over the whole structure (not the current view), so a box is
 * never dimmed just because the current scope hasn't drilled down to the
 * match nested inside it yet.
 */
export function subtreeSearchMatches(
  structure: DataflowStructure,
  search: string,
): SubtreeSearchMatches {
  const needle = search.trim().toLowerCase();
  const matches = new Set<NodeId>();
  const containsMatch = new Set<NodeId>();
  if (!needle) return { matches, containsMatch };
  const visit = (id: NodeId): boolean => {
    const node = mustGet(structure.nodes, id);
    let hit = node.name.toLowerCase().includes(needle);
    if (hit) matches.add(id);
    for (const c of node.children) {
      if (visit(c)) hit = true;
    }
    if (hit) containsMatch.add(id);
    return hit;
  };
  for (const id of mustGet(structure.nodes, structure.root).children) {
    visit(id);
  }
  return { matches, containsMatch };
}

function addressCompare(a: Address, b: Address): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

/**
 * The ordered, structure-wide match list prev/next cycles through: jumping
 * to a match may navigate the view (unlike decorateGraph's searchMatches,
 * which only ever lists what's already visible in the current scope).
 */
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

/**
 * Groups operator nodes by the LIR span covering them, keyed
 * `${exportId}/${lirId}`. One dataflow can back several exports, so entries
 * are not unique per lir id across exports.
 */
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

/**
 * Sums each member's OWN stats (not transitive): every operator belonging to
 * a LIR id appears in memberIds exactly once, including a parent LIR id's
 * descendants (buildDataflowStructure assigns an operator to every ancestor
 * span that contains it), so this matches EXPLAIN ANALYZE's own total_memory/
 * total_records/total_elapsed exactly without a separate query: the same
 * operator rows are already fetched for the graph itself.
 */
export function lirSummary(
  structure: DataflowStructure,
  memberIds: readonly NodeId[],
): NodeStats {
  const summary: NodeStats = {
    arrangementRecords: 0n,
    arrangementSize: 0n,
    elapsedNs: 0n,
    scheduleCount: 0n,
  };
  for (const id of memberIds) {
    const own = structure.nodes.get(id)?.own;
    if (!own) continue;
    summary.arrangementRecords += own.arrangementRecords;
    summary.arrangementSize += own.arrangementSize;
    summary.elapsedNs += own.elapsedNs;
    summary.scheduleCount += own.scheduleCount;
  }
  return summary;
}

/**
 * Arranges lirIndex's flat entries into a tree per export, following
 * parentLirId. A lir id is assigned once its own build (and so every
 * descendant's) completes, so within one export a higher lir id is never an
 * ancestor of a lower one. Sorting each level descending by lir id therefore
 * matches construction order without needing it as an explicit input, the
 * same convention EXPLAIN ANALYZE's own tree rendering uses.
 */
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

export interface LirGroupNode {
  id: NodeId; // `${exportId}/${lirId}`, matching lirIndex's key
  exportId: string;
  lirId: string;
  operator: string;
  nesting: number;
}

/**
 * A grouping layer over an already-derived VisibleGraph, the same way
 * decorateGraph layers dimming and color over it: nothing here replaces
 * `nodes`, so search, dimming, and click handling keep working against the
 * flat list untouched.
 */
export interface LirGrouping {
  groups: LirGroupNode[]; // outer groups appear before groups nested inside them
  parentOf: Map<NodeId, NodeId>; // a VisibleNode or LirGroupNode id -> its immediate enclosing group's id
}

/**
 * Groups a scope's direct children into the LIR tree that encloses them, for
 * drawing nested boxes on the canvas. LIR spans are contiguous ranges over
 * operator ids and properly nested: a region is always fully enclosed by a
 * single LIR node's range. A dataflow can back several exports, and a shared
 * subplan can carry lir entries from more than one of them at once; a
 * compound-node tree needs one parent chain per node, so this groups by the
 * lowest exportId (string-compared) present among the given nodes only. A
 * node whose only lir entries belong to a different export renders
 * ungrouped, alongside nodes with no lir data at all (e.g. ports).
 */
export function groupByLir(nodes: readonly VisibleNode[]): LirGrouping {
  const groups: LirGroupNode[] = [];
  const parentOf = new Map<NodeId, NodeId>();
  const ordered = nodes
    .filter(
      (n): n is VisibleNode & { operatorId: bigint } => n.operatorId !== null,
    )
    .slice()
    .sort((a, b) => {
      const x = a.operatorId;
      const y = b.operatorId;
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
        const info = node.lir.find((l) => `${l.exportId}/${l.lirId}` === key);
        if (!info) {
          // key was derived from this same node.lir list two lines up
          // (filtered to chosenExport, mapped to "exportId/lirId"), so the
          // matching entry always exists. This only guards the invariant.
          throw new Error(`missing lir info for key ${key}`);
        }
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
