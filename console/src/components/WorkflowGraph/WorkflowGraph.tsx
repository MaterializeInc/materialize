// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex } from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { keepPreviousData } from "@tanstack/react-query";
import React from "react";

import { DatabaseObject } from "~/api/materialize/objects";
import {
  SUBSCRIBE_ERROR_CODE,
  SubscribeError,
} from "~/api/materialize/SubscribeManager";
import { WorkflowGraphEdge } from "~/api/materialize/workflowGraph";
import {
  WorkflowGraphCanvasNode,
  WorkflowGraphNode,
} from "~/api/materialize/workflowGraphNodes";
import ErrorBox from "~/components/ErrorBox";
import { Canvas, GraphEdge, GraphEdgeContainer } from "~/components/Graph";
import { createWorkflowNodeBottomFn } from "~/components/Graph/dagreGraphHelpers";
import { useDagreGraph } from "~/hooks/useDagreGraph";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { WORKFLOW_GRAPH_CANVAS_Z_INDEX } from "~/layouts/zIndex";
import { useMaterializationLag } from "~/platform/clusters/queries";
import {
  useWorkflowGraphEdgesAvailable,
  useWorkflowGraphLive,
} from "~/store/workflowGraphEdgesCollection";
import { notNullOrUndefined } from "~/util";

import { AppErrorBoundary } from "../AppErrorBoundary";
import { LoadingContainer } from "../LoadingContainer";
import { getDownstreamNodes, getUpstreamNodes } from "./graph";
import {
  BASE_NODE_HEIGHT,
  GraphNode,
  NODE_HEIGHT_WITH_STATUS,
  NODE_WIDTH,
} from "./GraphNode";
import {
  useWorkflowGraph,
  useWorkflowGraphNodeDetails,
  useWorkflowGraphNodes,
} from "./queries";
import WorkflowGraphSidebar, { SIDEBAR_WIDTH } from "./Sidebar";

const WorkflowGraph = (props: {
  /** Object that we are generating a DAG for */
  focusedObjectId: string;
}) => {
  const edgesAvailable = useWorkflowGraphEdgesAvailable();
  return (
    <MainContentContainer justifyContent="center" m="0" p="0">
      <AppErrorBoundary message="An error occurred fetching workflow data.">
        <React.Suspense fallback={<LoadingContainer />}>
          {edgesAvailable === null ? (
            <LoadingContainer />
          ) : edgesAvailable ? (
            <SyncedWorkflowGraph {...props} />
          ) : (
            <WorkflowGraphInner {...props} />
          )}
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

const WorkflowGraphInner = ({
  focusedObjectId,
}: {
  focusedObjectId: string;
}) => {
  const {
    data: { rows: graphEdges },
  } = useWorkflowGraph({
    objectId: focusedObjectId,
  });

  const edgeIds = React.useMemo(() => {
    if (!graphEdges || graphEdges.length < 1)
      return focusedObjectId ? [focusedObjectId] : [];

    const ids = new Set<string>();
    for (const edge of graphEdges) {
      if (edge.parentId) ids.add(edge.parentId);
      if (edge.childId) ids.add(edge.childId);
    }
    return Array.from(ids.values());
  }, [focusedObjectId, graphEdges]);

  const {
    data: { rows: graphNodes },
  } = useWorkflowGraphNodes({
    objectIds: edgeIds,
  });

  const nodeMap = React.useMemo(() => {
    return new Map<string, WorkflowGraphNode>(
      graphNodes?.map((r) => [r.id, r]) ?? [],
    );
  }, [graphNodes]);

  if (graphNodes && graphNodes.length === 0) {
    // This should not happen in practice, even if an object is has no other dependencies
    // to show, the focused object should still show up. This generally happens if you
    // try to render the workflow graph for an object that is not valid object in the
    // workflow graph, e.g. a progress source.
    return <ErrorBox message="Unable to render workflow graph." />;
  }

  // this guards against the moment where the edges have not yet loaded,
  // areNodesLoading is false, since there is no query to run yet. I think we should
  // be able to remove it when we migrate to react query.
  if (graphNodes === null) {
    return <LoadingContainer />;
  }

  return (
    <WorkflowGraphView
      focusedObjectId={focusedObjectId}
      graphEdges={graphEdges ?? []}
      nodeMap={nodeMap}
    />
  );
};

/**
 * Catalog fields come from the catalog row: the details query refetches only
 * when the node ids change, so a rename would keep its old name.
 */
function toCanvasNode(
  object: DatabaseObject,
  details: WorkflowGraphNode | undefined,
): WorkflowGraphCanvasNode {
  return {
    id: object.id,
    name: object.name,
    databaseName: object.databaseName,
    schemaName: object.schemaName,
    type: object.objectType,
    clusterId: object.clusterId,
    clusterName: object.clusterName,
    sourceType: object.sourceType,
    createdAt: details?.createdAt ?? null,
    sourceStatus: details?.sourceStatus ?? null,
    sourceSnapshotCommitted: details?.sourceSnapshotCommitted ?? null,
    sinkStatus: details?.sinkStatus ?? null,
    connectionId: details?.connectionId ?? null,
    connectionName: details?.connectionName ?? null,
  };
}

/**
 * Whether a collection's error leaves nothing to draw. A dropped connection
 * keeps the rows while the SUBSCRIBE reconnects.
 */
const isFailure = (error: SubscribeError | undefined, holdsRows: boolean) =>
  error !== undefined &&
  (error.code !== SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED || !holdsRows);

/** The graph from the edges and catalog collections, without a query per open. */
const SyncedWorkflowGraph = ({
  focusedObjectId,
}: {
  focusedObjectId: string;
}) => {
  const {
    edges,
    nodeIds,
    objects,
    edgesReady,
    edgesError,
    objectsSnapshotComplete,
    objectsError,
  } = useWorkflowGraphLive(focusedObjectId);
  const { data: details, isError: isDetailsError } =
    useWorkflowGraphNodeDetails({
      objectIds: nodeIds,
    });

  // Inserted in `nodeIds` order, which is sorted, so dagre lays out the same
  // graph whatever order the collections return rows in.
  const nodeMap = React.useMemo(() => {
    const objectsById = new Map(objects.map((object) => [object.id, object]));
    const detailsById = new Map(details?.rows.map((row) => [row.id, row]));
    const map = new Map<string, WorkflowGraphCanvasNode>();
    for (const id of nodeIds) {
      const object = objectsById.get(id);
      if (object) map.set(id, toCanvasNode(object, detailsById.get(id)));
    }
    return map;
  }, [details, nodeIds, objects]);

  // An edge whose endpoint has no catalog row yet would make dagre invent a
  // sizeless node. Keyed on the drawn ids, like the view's nodes.
  const drawnIds = [...nodeMap.keys()].join(",");
  const graphEdges = React.useMemo(() => {
    const drawn = new Set(drawnIds.split(","));
    return edges.filter(
      (edge) => drawn.has(edge.parentId) && drawn.has(edge.childId),
    );
  }, [edges, drawnIds]);

  if (
    isFailure(edgesError, edgesReady) ||
    isFailure(objectsError, objectsSnapshotComplete)
  ) {
    return <ErrorBox message="An error occurred fetching workflow data." />;
  }

  const nodesReady = nodeIds.every((id) => nodeMap.has(id));
  if (!edgesReady || (!nodesReady && !objectsSnapshotComplete)) {
    return <LoadingContainer />;
  }

  const focusedNode = nodeMap.get(focusedObjectId);
  // Matches the SQL path, whose node query has no row for these.
  if (!focusedNode || focusedNode.sourceType === "progress") {
    return <ErrorBox message="Unable to render workflow graph." />;
  }

  return (
    <WorkflowGraphView
      focusedObjectId={focusedObjectId}
      graphEdges={graphEdges}
      nodeMap={nodeMap}
      isDetailsError={isDetailsError}
      drawBeforeLag
    />
  );
};

const WorkflowGraphView = ({
  focusedObjectId,
  graphEdges,
  nodeMap,
  isDetailsError,
  drawBeforeLag,
}: {
  focusedObjectId: string;
  graphEdges: WorkflowGraphEdge[];
  nodeMap: Map<string, WorkflowGraphCanvasNode>;
  isDetailsError?: boolean;
  /**
   * Draw before lag loads. Status bars and edge start points then move once,
   * when it lands.
   */
  drawBeforeLag?: boolean;
}) => {
  const [selectedNodeId, setSelectedNodeId] = React.useState<
    string | undefined
  >(undefined);

  // Keyed on the node ids, since `nodeMap` changes when details land, and a new
  // layout pans the canvas back to the selected node.
  const nodeIds = [...nodeMap.keys()].join(",");
  const objectsWithLag = React.useMemo(
    () => (nodeIds === "" ? [] : nodeIds.split(",")),
    [nodeIds],
  );

  const { data, isLoading: isLagmapLoading } = useMaterializationLag(
    { objectIds: objectsWithLag },
    // For the same reason as `useWorkflowGraphNodeDetails`.
    { placeholderData: drawBeforeLag ? keepPreviousData : undefined },
  );

  const { lagMap } = data ?? {};

  // Convert the nodes to DagreGraphNode format
  const nodes = React.useMemo(
    () =>
      objectsWithLag.map((id) => ({
        id,
        width: NODE_WIDTH,
        height: NODE_HEIGHT_WITH_STATUS,
      })),
    [objectsWithLag],
  );

  const getNodeBottom = React.useMemo(
    () =>
      createWorkflowNodeBottomFn(
        lagMap,
        nodeMap,
        BASE_NODE_HEIGHT,
        NODE_HEIGHT_WITH_STATUS,
      ),
    [lagMap, nodeMap],
  );

  const {
    graph,
    nodePositionMap,
    height,
    width,
    selectedNode,
    orderedGraphEdges,
    clampPoints,
  } = useDagreGraph({
    nodes,
    edges: graphEdges,
    selectedNodeId,
    ranksep: NODE_HEIGHT_WITH_STATUS * 2,
    getNodeBottom,
  });

  const upstreamNodes = React.useMemo(() => {
    if (!selectedNodeId || !graph) return [];
    return getUpstreamNodes(selectedNodeId, graph)
      .map((id) => nodeMap.get(id))
      .filter(notNullOrUndefined);
  }, [graph, nodeMap, selectedNodeId]);

  const downstreamNodes = React.useMemo(() => {
    if (!selectedNodeId || !graph) return [];
    return getDownstreamNodes(selectedNodeId, graph)
      .map((id) => nodeMap.get(id))
      .filter(notNullOrUndefined);
  }, [graph, nodeMap, selectedNodeId]);

  React.useEffect(() => {
    if (!selectedNodeId && focusedObjectId) {
      setSelectedNodeId(focusedObjectId);
    }
  }, [focusedObjectId, selectedNodeId]);

  if (
    !focusedObjectId ||
    !selectedNodeId ||
    !graph ||
    (isLagmapLoading && !drawBeforeLag)
  ) {
    return <LoadingContainer />;
  }

  // This should never happen in practice
  if (!width || !height) {
    Sentry.captureException(
      new Error("WorkflowGraph missing height and width"),
    );
    return <ErrorBox message="An error occurred rendering the workflow." />;
  }

  return (
    <Flex position="relative" height="100%" flex="1">
      <Canvas
        width={width}
        height={height}
        selectedNode={selectedNode}
        rightOffset={SIDEBAR_WIDTH}
        controlsZIndex={WORKFLOW_GRAPH_CANVAS_Z_INDEX}
      >
        {graph.nodes().map((id: string) => {
          const node = graph.node(id);
          const nodePosition = nodePositionMap.get(id);
          if (!node || !nodePosition) return null;

          return (
            <GraphNode
              key={id}
              graph={graph}
              nodeLagInfo={lagMap?.get(id)}
              node={nodeMap.get(id)}
              isSelected={id === selectedNodeId}
              left={nodePosition.left}
              top={nodePosition.top}
              width={node.width}
              onClick={() => {
                setSelectedNodeId(id);
              }}
            />
          );
        })}
        <GraphEdgeContainer width={width} height={height}>
          {orderedGraphEdges.map((e) => {
            const edge = graph.edge(e);
            const parentNodePosition = nodePositionMap.get(e.v);
            if (!edge || !parentNodePosition) return null;

            const [firstPoint, ...points] = clampPoints(edge.points);

            return (
              <GraphEdge
                key={e.v + e.w}
                from={graph.node(e.v)}
                to={graph.node(e.w)}
                points={[
                  // Because our node heights vary, we have to look up the value of the
                  // parent node
                  { x: firstPoint.x, y: parentNodePosition.bottom },
                  ...points,
                ]}
                isAdjacentToSelectedNode={
                  e.v === selectedNodeId || e.w === selectedNodeId
                }
              />
            );
          })}
        </GraphEdgeContainer>
      </Canvas>
      <WorkflowGraphSidebar
        selectedNode={nodeMap.get(selectedNodeId)}
        upstreamNodes={upstreamNodes}
        downstreamNodes={downstreamNodes}
        onNodeClick={setSelectedNodeId}
        nodeMap={nodeMap}
        isDetailsError={isDetailsError}
        lagInfo={selectedNodeId ? lagMap?.get(selectedNodeId) : undefined}
      />
    </Flex>
  );
};

export default WorkflowGraph;
