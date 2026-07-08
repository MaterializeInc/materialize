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
