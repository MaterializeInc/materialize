// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "@xyflow/react/dist/style.css";

import { Box, Spinner, useToast } from "@chakra-ui/react";
import {
  Background,
  Controls,
  type Edge,
  MiniMap,
  type Node,
  Position,
  ReactFlow,
  useReactFlow,
} from "@xyflow/react";
import React from "react";

import { ChannelEdge } from "./ChannelEdge";
import {
  type CollapseState,
  type DataflowStructure,
  deriveVisibleGraph,
  type GraphDecorations,
  MAX_VISIBLE_NODES,
  type VisibleNode,
  visibleNodeCount,
} from "./dataflowGraph";
import { NODE_DIMENSIONS } from "./elkGraph";
import {
  CollapsedRegionNode,
  OperatorNode,
  PortNode,
  RegionNode,
} from "./nodes";
import { nodeFillColor } from "./nodeStyle";
import { useElkLayout } from "./useElkLayout";

const edgeTypes = { channel: ChannelEdge };
const nodeTypes = {
  operator: OperatorNode,
  collapsedRegion: CollapsedRegionNode,
  region: RegionNode,
  port: PortNode,
};

export interface DataflowGraphViewProps {
  structure: DataflowStructure;
  collapsed: CollapseState;
  onCollapsedChange: (next: CollapseState) => void;
  cacheKey: string;
  decorations?: GraphDecorations;
  onNodeClick?: (node: VisibleNode) => void;
  onPaneClick?: () => void;
  centerRef?: React.MutableRefObject<((id: string) => void) | null>;
}

// Exposes a centering callback through the ref once React Flow context exists.
const CenterHelper = ({
  centerRef,
}: {
  centerRef: React.MutableRefObject<((id: string) => void) | null>;
}) => {
  const reactFlow = useReactFlow();
  React.useEffect(() => {
    // Assigning to the ref inside an effect is safe: it never runs during
    // render, so the parent's ref just receives the latest centering callback.
    // eslint-disable-next-line react-compiler/react-compiler
    centerRef.current = (id: string) => {
      const internal = reactFlow.getInternalNode(id);
      if (!internal) return;
      const { x, y } = internal.internals.positionAbsolute;
      const width = internal.measured?.width ?? 0;
      const height = internal.measured?.height ?? 0;
      reactFlow.setCenter(x + width / 2, y + height / 2, {
        zoom: 1,
        duration: 300,
      });
    };
    return () => {
      centerRef.current = null;
    };
  }, [reactFlow, centerRef]);
  return null;
};

export const DataflowGraphView = ({
  structure,
  collapsed,
  onCollapsedChange,
  cacheKey,
  decorations,
  onNodeClick,
  onPaneClick,
  centerRef,
}: DataflowGraphViewProps) => {
  const toast = useToast();
  const visible = React.useMemo(() => {
    const graph = deriveVisibleGraph(structure, collapsed);
    if (!decorations?.hiddenNodeIds && !decorations?.hiddenEdgeIds)
      return graph;
    const hiddenNodes = decorations.hiddenNodeIds ?? new Set();
    const hiddenEdges = decorations.hiddenEdgeIds ?? new Set();
    return {
      nodes: graph.nodes.filter((n) => !hiddenNodes.has(n.id)),
      edges: graph.edges.filter(
        (e) =>
          !hiddenEdges.has(e.id) &&
          !hiddenNodes.has(e.source) &&
          !hiddenNodes.has(e.target),
      ),
    };
  }, [
    structure,
    collapsed,
    decorations?.hiddenNodeIds,
    decorations?.hiddenEdgeIds,
  ]);

  const layoutKey = `${cacheKey}|${[...collapsed].sort().join(",")}|${
    decorations?.hiddenNodeIds
      ? [...decorations.hiddenNodeIds].sort().join(",")
      : ""
  }|${
    decorations?.hiddenEdgeIds
      ? [...decorations.hiddenEdgeIds].sort().join(",")
      : ""
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
        // Match the Top/Bottom handles so bezier control points meet the
        // node edges. Without this the edge curves toward the default
        // Bottom/Top and appears detached across region boundaries.
        targetPosition: Position.Top,
        sourcePosition: Position.Bottom,
        // Set as explicit node fields, not just CSS style: the MiniMap and
        // culled (onlyRenderVisibleElements) off-screen nodes both need a
        // known size without waiting for DOM measurement.
        width: pos.width,
        height: pos.height,
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
    [visible, decorations?.dimmedNodeIds, decorations?.dimmedEdgeIds],
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
        // Double-click is the collapse toggle, so it must not also zoom.
        zoomOnDoubleClick={false}
        onNodeClick={(_, node) =>
          onNodeClick?.((node.data as { node: VisibleNode }).node)
        }
        onPaneClick={onPaneClick}
        onNodeDoubleClick={(_, node) => {
          const visibleNode = (node.data as { node: VisibleNode }).node;
          if (
            visibleNode.kind === "region" ||
            visibleNode.kind === "collapsedRegion"
          ) {
            toggleRegion(visibleNode);
          }
        }}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) =>
            (node.data as { color?: string }).color ?? "#ccc"
          }
        />
        {centerRef && <CenterHelper centerRef={centerRef} />}
      </ReactFlow>
    </Box>
  );
};
