// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IPostgresInterval } from "~/api/materialize";
import { CriticalPathRow } from "~/api/materialize/maintained-objects/criticalPath";
import { sumPostgresIntervalMs } from "~/util";

import { CriticalPathData, MaintainedObjectListItem } from "./queries";

export type NodeKind = "primary" | "upstream" | "offPath" | "healthy";

export interface RenderableNode {
  id: string;
  name: string;
  lag: IPostgresInterval | null;
  isProbe: boolean;
  kind: NodeKind;
  /** Count of off-path sibling inputs sharing the same child as this node. */
  offPathCount: number;
  objectType: string | null;
  /** For subsources, the parent source id — link targets should use this. */
  parentSourceId: string | null;
  /** Cluster the node runs on. Null for objects without a cluster (tables). */
  cluster: { id: string; name: string } | null;
  /** Max lag this node observed in the lookback window. */
  peakLag: IPostgresInterval | null;
  /** Timestamp of `peakLag`. */
  peakLagAt: Date | null;
}

/** A node renders green ("healthy") when its lag is within this threshold. */
const HEALTHY_LAG_MS = 2000;

const lagMs = (lag: IPostgresInterval | null) =>
  lag ? sumPostgresIntervalMs(lag) : 0;

const isHealthyLag = (lag: IPostgresInterval | null) =>
  lag !== null && lagMs(lag) <= HEALTHY_LAG_MS;

/** Three Maps describing the critical-path subgraph.
 *
 *  Built once from `data.rows` so the rest of `buildGraphView` can look up
 *  node info, chain edges, and depth by id, instead of rescanning every row
 *  each time it needs them.
 *
 *   - `nodeById`            — keyed by node id, maps to a representative row
 *                             carrying the node's metadata (name, lag, type).
 *   - `chainParentsByChild` — keyed by child id, maps to the child's parent
 *                             ids on the critical path (off-path excluded).
 *   - `depthById`           — keyed by node id, maps to hops from the probe
 *                             along chain edges. Probe is depth 0.
 */
interface ChainMaps {
  nodeById: Map<string, CriticalPathRow>;
  chainParentsByChild: Map<string, string[]>;
  depthById: Map<string, number>;
}

const buildChainMaps = (
  rows: CriticalPathRow[],
  probeId: string,
): ChainMaps => {
  // Build map of node id → metadata row.
  const nodeById = new Map<string, CriticalPathRow>();
  // Build map of child id → its parent ids on the critical path.
  const chainParentsByChild = new Map<string, string[]>();
  for (const r of rows) {
    if (!nodeById.has(r.id)) nodeById.set(r.id, r);
    if (!r.isBottleneck) continue;
    const parents = chainParentsByChild.get(r.childId) ?? [];
    parents.push(r.id);
    chainParentsByChild.set(r.childId, parents);
  }

  // Build map of node id → hops from probe along chain edges (BFS upstream).
  // Queue carries (id, depth) tuples so we never need to re-look up depth.
  const depthById = new Map<string, number>([[probeId, 0]]);
  const queue: [string, number][] = [[probeId, 0]];
  let head = 0;
  while (head < queue.length) {
    const [childId, childDepth] = queue[head++];
    for (const parentId of chainParentsByChild.get(childId) ?? []) {
      if (depthById.has(parentId)) continue;
      depthById.set(parentId, childDepth + 1);
      queue.push([parentId, childDepth + 1]);
    }
  }

  return { nodeById, chainParentsByChild, depthById };
};

/** Returns every chain node tied at the largest self-delay (own lag − max parent lag). */
const findPrimaryBottlenecks = (
  lagMsByChainId: Map<string, number>,
  chainParentsByChild: Map<string, string[]>,
  probeLagMs: number,
): Set<string> => {
  if (probeLagMs <= 0) return new Set();

  const withSelfDelay = [...lagMsByChainId.entries()].map(([id, lag]) => {
    const parents = chainParentsByChild.get(id) ?? [];
    const maxParentLag = Math.max(
      0,
      ...parents.map((p) => lagMsByChainId.get(p) ?? 0),
    );
    return { id, selfDelay: lag - maxParentLag };
  });

  const maxSelfDelay = Math.max(0, ...withSelfDelay.map((x) => x.selfDelay));
  if (maxSelfDelay <= 0) return new Set();

  return new Set(
    withSelfDelay.filter((x) => x.selfDelay === maxSelfDelay).map((x) => x.id),
  );
};

const classifyNode = ({
  id,
  lag,
  primaryIds,
}: {
  id: string;
  lag: IPostgresInterval | null;
  primaryIds: Set<string>;
}): NodeKind => {
  if (isHealthyLag(lag)) return "healthy";
  if (primaryIds.has(id)) return "primary";
  return "upstream";
};

/** For each visible chain node, count its non-bottleneck siblings. */
const computeOffPathCounts = (
  rows: CriticalPathRow[],
  chainNodeIds: string[],
): Map<string, number> => {
  const result = new Map<string, number>();
  for (const id of chainNodeIds) {
    const childId = rows.find((r) => r.id === id && r.isBottleneck)?.childId;
    if (!childId) continue;
    result.set(
      id,
      rows.filter((r) => r.childId === childId && !r.isBottleneck).length,
    );
  }
  return result;
};

export interface GraphView {
  nodes: RenderableNode[];
  edges: CriticalPathRow[];
  /** Chain nodes past `visibleDepth`. Drives the "Show N more" CTA. */
  hiddenChainCount: number;
}

/** Turns the critical-path query result into the DAG the UI renders.
 *
 *  Three things happen here:
 *
 *   1. Chain — pick the queried object plus its critical-path ancestors, cut
 *      off at `visibleDepth` hops. Anything past that becomes `hiddenChainCount`
 *      and drives the "Show N more upstream" CTA.
 *   2. Primary marker — tag the chain node with the largest self-delay as
 *      `primary` so it renders red.
 *   3. Off-path expansion — if `expandedBottleneckId` is set, splice that node's
 *      non-bottleneck siblings in as extra nodes/edges so the user can see what
 *      other inputs feed the same downstream child.
 */
export const buildGraphView = (
  data: CriticalPathData,
  probe: MaintainedObjectListItem,
  expandedBottleneckId: string | null,
  visibleDepth: number,
): GraphView => {
  const { nodeById, chainParentsByChild, depthById } = buildChainMaps(
    data.rows,
    probe.id,
  );
  const isVisibleChainNode = (id: string) => {
    const d = depthById.get(id);
    return d !== undefined && d <= visibleDepth;
  };

  // Visible chain nodes (probe itself excluded — it's added separately as the
  // probe node).
  const visibleChainIds = [...depthById.keys()].filter(
    (id) => id !== probe.id && isVisibleChainNode(id),
  );
  // −1 to exclude the probe itself, which is at depth 0.
  const hiddenChainCount = depthById.size - visibleChainIds.length - 1;

  const probeLag = probe.lag?.value ?? null;
  const probeLagMs = lagMs(probeLag);
  const allChainIds = [...depthById.keys()].filter((id) => id !== probe.id);
  const lagMsByChainId = new Map<string, number>([
    ...allChainIds.map(
      (id) => [id, lagMs(nodeById.get(id)?.lag ?? null)] as const,
    ),
    [probe.id, probeLagMs],
  ]);
  const primaryIds = findPrimaryBottlenecks(
    lagMsByChainId,
    chainParentsByChild,
    probeLagMs,
  );

  const offPathCountById = computeOffPathCounts(data.rows, visibleChainIds);

  // Read the probe's peak from any row mentioning it as source or target.
  const probeRow =
    data.rows.find((r) => r.id === probe.id) ??
    data.rows.find((r) => r.childId === probe.id);
  const probeNode: RenderableNode = {
    id: probe.id,
    name: probe.name,
    lag: probeLag,
    isProbe: true,
    kind: classifyNode({
      id: probe.id,
      lag: probeLag,
      primaryIds,
    }),
    offPathCount: 0,
    objectType: probe.objectType,
    parentSourceId: null,
    cluster: probe.cluster,
    peakLag: probeRow?.peakLag ?? null,
    peakLagAt: probeRow?.peakLagAt ?? null,
  };
  const chainNodes: RenderableNode[] = visibleChainIds.flatMap((id) => {
    const r = nodeById.get(id);
    if (!r) return [];
    return [
      {
        id,
        name: r.name,
        lag: r.lag,
        isProbe: false,
        kind: classifyNode({
          id,
          lag: r.lag,
          primaryIds,
        }),
        offPathCount: offPathCountById.get(id) ?? 0,
        objectType: r.objectType,
        parentSourceId: r.parentSourceId,
        cluster:
          r.clusterId && r.clusterName
            ? { id: r.clusterId, name: r.clusterName }
            : null,
        peakLag: r.peakLag,
        peakLagAt: r.peakLagAt,
      },
    ];
  });
  const chainEdges = data.rows.filter(
    (r) =>
      r.isBottleneck &&
      isVisibleChainNode(r.id) &&
      isVisibleChainNode(r.childId),
  );

  // No bottleneck expanded → chain only.
  if (!expandedBottleneckId || !isVisibleChainNode(expandedBottleneckId)) {
    return {
      nodes: [probeNode, ...chainNodes],
      edges: chainEdges,
      hiddenChainCount,
    };
  }

  // Splice in the expanded bottleneck's off-path siblings.
  const expandedChildId = data.rows.find(
    (r) => r.id === expandedBottleneckId && r.isBottleneck,
  )?.childId;
  if (!expandedChildId) {
    return {
      nodes: [probeNode, ...chainNodes],
      edges: chainEdges,
      hiddenChainCount,
    };
  }
  const offPathEdges = data.rows.filter(
    (r) => r.childId === expandedChildId && !r.isBottleneck,
  );
  const offPathNodes: RenderableNode[] = offPathEdges.map((r) => ({
    id: r.id,
    name: r.name,
    lag: r.lag,
    isProbe: false,
    kind: "offPath",
    offPathCount: 0,
    objectType: r.objectType,
    parentSourceId: r.parentSourceId,
    cluster:
      r.clusterId && r.clusterName
        ? { id: r.clusterId, name: r.clusterName }
        : null,
    peakLag: r.peakLag,
    peakLagAt: r.peakLagAt,
  }));

  return {
    nodes: [probeNode, ...chainNodes, ...offPathNodes],
    edges: [...chainEdges, ...offPathEdges],
    hiddenChainCount,
  };
};
