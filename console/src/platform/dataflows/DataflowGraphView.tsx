// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import "@xyflow/react/dist/style.css";
import "./DataflowGraphView.css";

import { Box, Button, Spinner, VStack } from "@chakra-ui/react";
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

import ErrorBox from "~/components/ErrorBox";

import { ChannelEdge } from "./ChannelEdge";
import {
  type GraphDecorations,
  type NodeId,
  type PortPeer,
  rerouteHiddenNodes,
  type VisibleEdge,
  type VisibleGraph,
  type VisibleNode,
} from "./dataflowGraph";
import { NODE_DIMENSIONS } from "./elkGraph";
import { OperatorNode, PortNode, RegionNode } from "./nodes";
import { nodeFillColor } from "./nodeStyle";
import { useElkLayout } from "./useElkLayout";

const edgeTypes = { channel: ChannelEdge };
const nodeTypes = {
  operator: OperatorNode,
  region: RegionNode,
  port: PortNode,
};

// An edge plus resolved endpoint labels, since VisibleEdge only carries ids.
export type SelectedEdge = VisibleEdge & {
  sourceLabel: string;
  targetLabel: string;
};

export interface DataflowGraphViewProps {
  // The current scope's graph, already derived by the caller (which also
  // needs it, to decorate). Computing it again here would repeat the same
  // work every render.
  visible: VisibleGraph;
  // The scope whose direct children this view renders. Double-clicking a
  // region box navigates to a new view rooted there rather than expanding it
  // in place.
  focusedScope: NodeId;
  onNavigate: (scope: NodeId) => void;
  cacheKey: string;
  decorations?: GraphDecorations;
  selectedId?: string;
  activeMatchId?: string;
  // Edges connected to the clicked node, with endpoint labels resolved, so a
  // clicked port can show what flows through it without the caller
  // recomputing the (already decorated, rerouted) visible graph.
  onNodeClick?: (node: VisibleNode, connectedEdges: SelectedEdge[]) => void;
  onEdgeClick?: (edge: SelectedEdge) => void;
  onPaneClick?: () => void;
  // Double-clicking a port with exactly one peer jumps straight there. With
  // zero or several peers it's ambiguous (or there's nothing to jump to), so
  // the preceding click/click of the double-click has already opened the
  // port's own detail panel instead, same as a single click would.
  onJumpToPeer?: (peer: PortPeer) => void;
  centerRef?: React.MutableRefObject<((id: string) => void) | null>;
  // A node to center on once its layout position exists (e.g. right after an
  // expand triggered by jumping to it). Centering an id whose ancestors were
  // just expanded can't happen synchronously: layout runs in a worker, so
  // this id is retried across renders until its position shows up.
  centerOnId?: string | null;
  onCentered?: () => void;
  // Fits the viewport around a whole set of nodes (e.g. every operator
  // belonging to one LIR id), as opposed to centerRef's single-node zoom.
  fitRef?: React.MutableRefObject<((ids: string[]) => void) | null>;
  fitOnIds?: string[] | null;
  onFit?: () => void;
}

// Exposes centering/fitting callbacks through refs once React Flow context
// exists.
const CenterHelper = ({
  centerRef,
  fitRef,
}: {
  centerRef: React.MutableRefObject<((id: string) => void) | null>;
  fitRef?: React.MutableRefObject<((ids: string[]) => void) | null>;
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
    if (fitRef) {
      // eslint-disable-next-line react-compiler/react-compiler
      fitRef.current = (ids: string[]) => {
        void reactFlow.fitView({
          nodes: ids.map((id) => ({ id })),
          duration: 300,
          padding: 0.3,
          maxZoom: 1,
        });
      };
    }
    return () => {
      centerRef.current = null;
      if (fitRef) fitRef.current = null;
    };
  }, [reactFlow, centerRef, fitRef]);
  return null;
};

export const DataflowGraphView = ({
  visible: rawVisible,
  focusedScope,
  onNavigate,
  cacheKey,
  decorations,
  selectedId,
  activeMatchId,
  onNodeClick,
  onEdgeClick,
  onPaneClick,
  onJumpToPeer,
  centerRef,
  centerOnId,
  onCentered,
  fitRef,
  fitOnIds,
  onFit,
}: DataflowGraphViewProps) => {
  const visible = React.useMemo(() => {
    // Hidden nodes get spliced out with connectivity preserved (a hidden idle
    // run still shows a pass-through edge). Hidden edges (independently
    // zero-message) are a plain removal: nothing to reroute since both
    // endpoints stay visible.
    const rerouted = decorations?.hiddenNodeIds
      ? rerouteHiddenNodes(rawVisible, decorations.hiddenNodeIds)
      : rawVisible;
    if (!decorations?.hiddenEdgeIds?.size) return rerouted;
    return {
      nodes: rerouted.nodes,
      edges: rerouted.edges.filter(
        (e) => !decorations.hiddenEdgeIds!.has(e.id),
      ),
    };
  }, [rawVisible, decorations?.hiddenNodeIds, decorations?.hiddenEdgeIds]);

  const layoutKey = `${cacheKey}|${focusedScope}|${
    decorations?.hiddenNodeIds
      ? [...decorations.hiddenNodeIds].sort().join(",")
      : ""
  }|${
    decorations?.hiddenEdgeIds
      ? [...decorations.hiddenEdgeIds].sort().join(",")
      : ""
  }`;
  const { positions, layouting, error, retry } = useElkLayout(
    visible,
    layoutKey,
  );

  React.useEffect(() => {
    if (!centerOnId || !positions?.[centerOnId]) return;
    // CenterHelper assigns centerRef.current in its own mount effect; on the
    // rare commit where that hasn't run yet, skip without marking the
    // request consumed (onCentered stays uncalled) so the next positions or
    // centerOnId change gets another chance, rather than silently dropping
    // the jump forever.
    if (!centerRef?.current) return;
    centerRef.current(centerOnId);
    onCentered?.();
  }, [centerOnId, positions, centerRef, onCentered]);

  React.useEffect(() => {
    if (!fitOnIds || fitOnIds.length === 0) return;
    // Fires as soon as any targets have a position rather than waiting for
    // every one, so a partially-expanded set (e.g. one member still hidden
    // behind a filter) doesn't block the fit indefinitely.
    const present = fitOnIds.filter((id) => positions?.[id]);
    if (present.length === 0) return;
    if (!fitRef?.current) return;
    fitRef.current(present);
    onFit?.();
  }, [fitOnIds, positions, fitRef, onFit]);

  const nodes: Node[] = React.useMemo(() => {
    if (!positions) return [];
    return visible.nodes.map((n) => {
      const pos = positions[n.id] ?? { x: 0, y: 0, ...NODE_DIMENSIONS[n.kind] };
      return {
        id: n.id,
        type: n.kind,
        position: { x: pos.x, y: pos.y },
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
          selected: n.id === selectedId,
          activeMatch: n.id === activeMatchId,
        },
      };
    });
  }, [
    visible,
    positions,
    decorations?.dimmedNodeIds,
    decorations?.nodeColors,
    selectedId,
    activeMatchId,
  ]);

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
            (decorations?.dimmedNodeIds?.has(e.target) ?? false),
          selected: e.id === selectedId,
        },
      })),
    [visible, decorations?.dimmedNodeIds, selectedId],
  );

  // Built once per visible-graph change instead of a .find() per connected
  // edge in the click handlers below, which is O(nodes) per edge and adds up
  // for a hub node in a large scope.
  const labelById = React.useMemo(
    () => new Map(visible.nodes.map((n) => [n.id, n.label])),
    [visible],
  );

  if (error) {
    return (
      <VStack width="100%" flex="1" alignItems="flex-start" spacing={2} p={4}>
        <ErrorBox message="There was an error laying out the dataflow graph" />
        <Button size="sm" onClick={retry}>
          Retry
        </Button>
      </VStack>
    );
  }
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
        // Double-click navigates into a region, so it must not also zoom.
        zoomOnDoubleClick={false}
        onNodeClick={(_, node) => {
          const visibleNode = (node.data as { node: VisibleNode }).node;
          const connectedEdges = visible.edges
            .filter(
              (e) => e.source === visibleNode.id || e.target === visibleNode.id,
            )
            .map((e) => ({
              ...e,
              sourceLabel: labelById.get(e.source) ?? e.source,
              targetLabel: labelById.get(e.target) ?? e.target,
            }));
          onNodeClick?.(visibleNode, connectedEdges);
        }}
        onEdgeClick={(_, edge) => {
          const e = visible.edges.find((ve) => ve.id === edge.id);
          if (!e) return;
          onEdgeClick?.({
            ...e,
            sourceLabel: labelById.get(e.source) ?? e.source,
            targetLabel: labelById.get(e.target) ?? e.target,
          });
        }}
        onPaneClick={onPaneClick}
        onNodeDoubleClick={(_, node) => {
          const visibleNode = (node.data as { node: VisibleNode }).node;
          if (visibleNode.kind === "region") {
            onNavigate(visibleNode.id);
          } else if (
            visibleNode.kind === "port" &&
            visibleNode.peers.length === 1
          ) {
            onJumpToPeer?.(visibleNode.peers[0]);
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
        {centerRef && <CenterHelper centerRef={centerRef} fitRef={fitRef} />}
      </ReactFlow>
    </Box>
  );
};
