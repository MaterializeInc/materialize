// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { inArray, useLiveQuery } from "@tanstack/react-db";
import { useAtomValue } from "jotai";
import React from "react";

import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import { DatabaseObject } from "~/api/materialize/objects";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import {
  buildWorkflowGraphEdgesQuery,
  OBJECT_GRAPH_EDGES_MIN_VERSION,
  parseWorkflowGraphEdgeKey,
  workflowGraphEdgeKey,
  WorkflowGraphEdgeRow,
} from "~/api/materialize/workflowGraphEdges";
import { traverseWorkflowGraph } from "~/api/materialize/workflowGraphTraversal";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

/** The environment's dependency edges, from `mz_object_graph_edges`. */
export const workflowGraphEdgesCollection =
  createSubscribeCollection<WorkflowGraphEdgeRow>({
    id: "workflow-graph-edges",
    getKey: workflowGraphEdgeKey,
    persistName: "workflow-graph-edges",
  });

const WORKFLOW_GRAPH_EDGES_SUBSCRIBE_OPTIONS = {
  target: workflowGraphEdgesCollection,
  scopeAtom: syncEngineCacheScopeLoadableAtom,
  subscribe: buildSubscribeQuery(buildWorkflowGraphEdgesQuery(), {
    upsertKey: ["childId", "parentId"],
  }),
  select: (row: SubscribeRow<WorkflowGraphEdgeRow>) => row.data,
  upsertKey: (row: SubscribeRow<WorkflowGraphEdgeRow>) =>
    workflowGraphEdgeKey(row.data),
};

const WORKFLOW_GRAPH_EDGES_SUBSCRIBE_DISABLED = {
  ...WORKFLOW_GRAPH_EDGES_SUBSCRIBE_OPTIONS,
  target: undefined,
};

/**
 * Whether the environment has the edges the collection subscribes to, or null
 * while that is unknown. The version is only known while the environment is
 * healthy, so held edges keep this true through a failed health check, which
 * would otherwise drop the SUBSCRIBE and replace the graph with a spinner.
 */
export function useWorkflowGraphEdgesAvailable(): boolean | null {
  const meetsVersion = useEnvironmentGate(OBJECT_GRAPH_EDGES_MIN_VERSION);
  const { snapshotComplete } = useAtomValue(
    workflowGraphEdgesCollection.statusAtom,
  );
  return meetsVersion ?? (snapshotComplete ? true : null);
}

/**
 * Holds the edges SUBSCRIBE while the caller is mounted. Each mount opens its
 * own SUBSCRIBE, so mount it on the route that owns the graph pages.
 */
export function useSubscribeToWorkflowGraphEdgesCollection() {
  const available = useWorkflowGraphEdgesAvailable();
  useGlobalSubscribeCollection(
    available === true
      ? WORKFLOW_GRAPH_EDGES_SUBSCRIBE_OPTIONS
      : WORKFLOW_GRAPH_EDGES_SUBSCRIBE_DISABLED,
  );
}

const NO_OBJECTS: DatabaseObject[] = [];

/**
 * The object's upstream and downstream edges, walked in the browser over the
 * edges collection, and the catalog rows of every node on them. Edges and node
 * ids are sorted.
 */
export function useWorkflowGraphLive(objectId: string) {
  const { data: allEdges } = useLiveQuery((q) =>
    q.from({ edges: workflowGraphEdgesCollection.collection }),
  );
  const edgesStatus = useAtomValue(workflowGraphEdgesCollection.statusAtom);
  const objectsStatus = useAtomValue(allObjectsCollection.statusAtom);

  // Any DDL in the environment changes `allEdges`. Memoizing on the walked
  // edges' keys keeps the same arrays while this graph is unchanged, since a
  // new array re-lays out the graph and pans the canvas back to its focus.
  const walkedKeys = React.useMemo(
    () =>
      traverseWorkflowGraph(allEdges ?? [], objectId)
        .map(workflowGraphEdgeKey)
        .sort()
        .join(","),
    [allEdges, objectId],
  );
  const edges: WorkflowGraphEdgeRow[] = React.useMemo(
    () =>
      walkedKeys === ""
        ? []
        : walkedKeys.split(",").map(parseWorkflowGraphEdgeKey),
    [walkedKeys],
  );
  const nodeIds = React.useMemo(() => {
    const ids = new Set<string>([objectId]);
    for (const edge of edges) {
      ids.add(edge.parentId);
      ids.add(edge.childId);
    }
    return [...ids].sort();
  }, [edges, objectId]);

  const { data: objects } = useLiveQuery(
    (q) =>
      q
        .from({ object: allObjectsCollection.collection })
        .where(({ object }) => inArray(object.id, nodeIds)),
    [nodeIds],
  );

  return {
    edges,
    nodeIds,
    objects: objects ?? NO_OBJECTS,
    edgesReady: edgesStatus.snapshotComplete,
    edgesRefreshing: edgesStatus.isRefreshing,
    edgesError: edgesStatus.error,
    objectsSnapshotComplete: objectsStatus.snapshotComplete,
    objectsRefreshing: objectsStatus.isRefreshing,
    objectsError: objectsStatus.error,
  };
}
