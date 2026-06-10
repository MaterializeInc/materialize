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
import React from "react";

import { WorkflowGraphNode } from "~/api/materialize/workflowGraphNodes";
import ErrorBox from "~/components/ErrorBox";
import { Canvas, GraphEdge, GraphEdgeContainer } from "~/components/Graph";
import { createWorkflowNodeBottomFn } from "~/components/Graph/dagreGraphHelpers";
import { useDagreGraph } from "~/hooks/useDagreGraph";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { WORKFLOW_GRAPH_CANVAS_Z_INDEX } from "~/layouts/zIndex";
import { useMaterializationLag } from "~/platform/clusters/queries";
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
import { useWorkflowGraph, useWorkflowGraphNodes } from "./queries";
import WorkflowGraphSidebar, { SIDEBAR_WIDTH } from "./Sidebar";

const WorkflowGraph = (props: {
  /** Object that we are generating a DAG for */
  focusedObjectId: string;
}) => {
  return (
    <MainContentContainer justifyContent="center" m="0" p="0">
      <AppErrorBoundary message="An error occurred fetching workflow data.">
        <React.Suspense fallback={<LoadingContainer />}>
          <WorkflowGraphInner {...props} />
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
  const [selectedNodeId, setSelectedNodeId] = React.useState<
    string | undefined
  >(undefined);

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

  const objectsWithLag = React.useMemo(() => {
    return graphNodes?.map((node) => node.id) ?? [];
  }, [graphNodes]);

  const { data, isLoading: isLagmapLoading } = useMaterializationLag({
    objectIds: objectsWithLag,
  });

  const { lagMap } = data ?? {};

  const nodeMap = React.useMemo(() => {
    return new Map<string, WorkflowGraphNode>(
      graphNodes?.map((r) => [r.id, r]) ?? [],
    );
  }, [graphNodes]);

  // Convert graphNodes to DagreGraphNode format
  const nodes = React.useMemo(
    () =>
      graphNodes?.map((node) => ({
        id: node.id,
        width: NODE_WIDTH,
        height: NODE_HEIGHT_WITH_STATUS,
      })) ?? [],
    [graphNodes],
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

  if (graphNodes && graphNodes.length === 0) {
    // This should not happen in practice, even if an object is has no other dependencies
    // to show, the focused object should still show up. This generally happens if you
    // try to render the workflow graph for an object that is not valid object in the
    // workflow graph, e.g. a progress source.
    return <ErrorBox message="Unable to render workflow graph." />;
  }

  if (
    !focusedObjectId ||
    !selectedNodeId ||
    !graph ||
    // this guards against the moment where the edges have not yet loaded,
    // areNodesLoading is false, since there is no query to run yet. I think we should
    // be able to remove it when we migrate to react query.
    graphNodes === null ||
    isLagmapLoading
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
        lagInfo={selectedNodeId ? lagMap?.get(selectedNodeId) : undefined}
      />
    </Flex>
  );
};

export default WorkflowGraph;
