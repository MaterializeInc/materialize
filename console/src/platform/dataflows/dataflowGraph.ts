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
  operator: string;
}

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
        .map(({ exportId, lirId, operator }) => ({
          exportId,
          lirId,
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

// A channel endpoint address ending in 0 denotes the enclosing scope's
// input (from side) or output (to side) port.
function endpointId(
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
