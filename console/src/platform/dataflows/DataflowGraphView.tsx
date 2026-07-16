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

import { Box, Button, Spinner, useTheme, VStack } from "@chakra-ui/react";
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
import { MaterializeTheme } from "~/theme";

import { ChannelEdge } from "./ChannelEdge";
import {
  type GraphDecorations,
  groupByLir,
  type LirGroupNode as LirGroupNodeData,
  type NodeId,
  type PortPeer,
  rerouteHiddenNodes,
  type VisibleEdge,
  type VisibleGraph,
  type VisibleNode,
} from "./dataflowGraph";
import {
  NODE_DIMENSIONS,
  type Positions,
  resolveAbsolutePositions,
} from "./elkGraph";
import {
  LirGroupNode as LirGroupNodeComponent,
  OperatorNode,
  PortNode,
  RegionNode,
} from "./nodes";
import { hexToRgba, lirGroupColor, nodeFillColor } from "./nodeStyle";
import { useElkLayout } from "./useElkLayout";

const edgeTypes = { channel: ChannelEdge };
const nodeTypes = {
  operator: OperatorNode,
  region: RegionNode,
  port: PortNode,
  lirGroup: LirGroupNodeComponent,
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
  // Toggled from the toolbar; grouping is computed here (from the same
  // post-hideIdle, post-reroute node list elk actually lays out) rather than
  // by the caller, so a hidden node can never end up as a group's only
  // member.
  showLirGroups?: boolean;
  onLirGroupClick?: (group: LirGroupNodeData) => void;
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

// elk lays each scope out independently, starting near its own origin, so a
// viewport pan/zoom left over from a previous, unrelated scope (or from the
// user having panned away) can land on a spot with nothing from the new
// scope in it at all. Once this scope's layout lands, if none of its nodes
// are actually on screen, refit rather than leaving the canvas looking
// empty. Checked once per scope (skipping the initial one, which React
// Flow's own `fitView` prop already covers), not on every render, so
// panning away deliberately after landing doesn't get fought.
const ViewportGuard = ({
  focusedScope,
  nodes,
  positions,
  paneRef,
}: {
  focusedScope: NodeId;
  nodes: VisibleNode[];
  positions: Positions | null;
  paneRef: React.RefObject<HTMLDivElement>;
}) => {
  const reactFlow = useReactFlow();
  const checkedScopeRef = React.useRef<NodeId | null>(focusedScope);
  React.useEffect(() => {
    if (!positions || nodes.length === 0) return;
    if (checkedScopeRef.current === focusedScope) return;
    checkedScopeRef.current = focusedScope;
    const pane = paneRef.current;
    if (!pane) return;
    const rect = pane.getBoundingClientRect();
    const { x, y, zoom } = reactFlow.getViewport();
    const visMinX = -x / zoom;
    const visMinY = -y / zoom;
    const visMaxX = visMinX + rect.width / zoom;
    const visMaxY = visMinY + rect.height / zoom;
    const anyVisible = nodes.some((n) => {
      const p = positions[n.id];
      return (
        p &&
        p.x < visMaxX &&
        p.x + p.width > visMinX &&
        p.y < visMaxY &&
        p.y + p.height > visMinY
      );
    });
    if (!anyVisible) void reactFlow.fitView({ padding: 0.3, duration: 300 });
  }, [focusedScope, nodes, positions, paneRef, reactFlow]);
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
  showLirGroups,
  onLirGroupClick,
}: DataflowGraphViewProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const visible = React.useMemo(() => {
    // Hidden nodes get spliced out with connectivity preserved (a hidden idle
    // run still shows a pass-through edge). Hidden edges (independently
    // zero-message) are a plain removal: nothing to reroute since both
    // endpoints stay visible.
    const rerouted = decorations?.hiddenNodeIds
      ? rerouteHiddenNodes(rawVisible, decorations.hiddenNodeIds)
      : rawVisible;
    const hiddenEdgeIds = decorations?.hiddenEdgeIds;
    if (!hiddenEdgeIds?.size) return rerouted;
    return {
      nodes: rerouted.nodes,
      edges: rerouted.edges.filter((e) => !hiddenEdgeIds.has(e.id)),
    };
  }, [rawVisible, decorations?.hiddenNodeIds, decorations?.hiddenEdgeIds]);

  const grouping = React.useMemo(
    () => (showLirGroups ? groupByLir(visible.nodes) : null),
    [showLirGroups, visible.nodes],
  );

  const layoutKey = `${cacheKey}|${focusedScope}|${showLirGroups ?? false}|${
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
    grouping ?? undefined,
  );

  // Only ViewportGuard needs this: everything else either reads positions
  // relative to parentId (React Flow's own convention, matching elk's) or
  // resolves absolute position through React Flow's internal APIs.
  const absolutePositions = React.useMemo(
    () =>
      positions
        ? resolveAbsolutePositions(positions, grouping?.parentOf)
        : null,
    [positions, grouping],
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
    const parentOf = grouping?.parentOf;
    const groupNodes: Node[] = (grouping?.groups ?? []).map((g) => {
      const pos = positions[g.id] ?? { x: 0, y: 0, width: 0, height: 0 };
      return {
        id: g.id,
        type: "lirGroup",
        position: { x: pos.x, y: pos.y },
        parentId: parentOf?.get(g.id),
        width: pos.width,
        height: pos.height,
        // pointerEvents: "none" on React Flow's own node wrapper (not just
        // inside LirGroupNode's own JSX) is the other half of the
        // click-through contract LirGroupNode's comment documents: the
        // wrapper defaults to pointer-events:all and LirGroupNode can't
        // override an ancestor it doesn't render. With the wrapper opted
        // out, LirGroupNode's header (pointerEvents:"auto") still receives
        // clicks (a descendant can always re-enable itself), and a click on
        // the group's empty body falls through to a member's own wrapper
        // when one is there, or further to the pane when none is.
        style: { width: pos.width, height: pos.height, pointerEvents: "none" },
        draggable: false,
        connectable: false,
        // Below its members, which draw over it; React Flow requires a
        // group node to precede its children in this array, which the
        // groupNodes-then-memberNodes concat below already guarantees, but
        // an explicit zIndex also protects against member nodes elsewhere
        // in the array being reordered by a future change.
        zIndex: -1,
        data: {
          group: g,
          label: g.operator,
          color: lirGroupColor(g.lirId, colors.lineGraph),
        },
      };
    });
    const memberNodes: Node[] = visible.nodes.map((n) => {
      const pos = positions[n.id] ?? { x: 0, y: 0, ...NODE_DIMENSIONS[n.kind] };
      return {
        id: n.id,
        type: n.kind,
        position: { x: pos.x, y: pos.y },
        parentId: grouping?.parentOf.get(n.id),
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
          color:
            decorations?.nodeColors?.get(n.id) ??
            nodeFillColor(n, colors.lineGraph, {
              arranged: colors.accent.purple,
              notArranged: colors.accent.green,
            }),
          selected: n.id === selectedId,
          activeMatch: n.id === activeMatchId,
        },
      };
    });
    return [...groupNodes, ...memberNodes];
  }, [
    visible,
    positions,
    grouping,
    decorations?.dimmedNodeIds,
    decorations?.nodeColors,
    selectedId,
    activeMatchId,
    colors,
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
          // selectedId is a node id when a node (rather than an edge) is
          // selected; an edge's source/target are always node ids too, so
          // this naturally never matches while an edge itself is selected,
          // with no need to separately track which kind selectedId is.
          connected: e.source === selectedId || e.target === selectedId,
          sourceLandings: e.sourceLandings,
          targetLandings: e.targetLandings,
          onJumpTo: onJumpToPeer,
        },
      })),
    [visible, decorations?.dimmedNodeIds, selectedId, onJumpToPeer],
  );

  // Built once per visible-graph change instead of a .find() per connected
  // edge in the click handlers below, which is O(nodes) per edge and adds up
  // for a hub node in a large scope.
  const labelById = React.useMemo(
    () => new Map(visible.nodes.map((n) => [n.id, n.label])),
    [visible],
  );

  const paneRef = React.useRef<HTMLDivElement>(null);

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
    <Box
      ref={paneRef}
      width="100%"
      flex="1"
      position="relative"
      // React Flow's Background/Controls/MiniMap render their own chrome
      // from a packaged stylesheet, outside Chakra's component tree, and
      // read colors through these CSS custom properties (documented,
      // themeable hooks -- not an internal implementation detail) rather
      // than props, so this is how they pick up light/dark mode.
      style={
        {
          "--xy-background-color": colors.background.primary,
          "--xy-background-pattern-color": colors.border.secondary,
          "--xy-minimap-background-color": colors.background.secondary,
          // Must stay translucent: this masks everything outside the
          // current viewport, and an opaque fill here hides every node dot
          // under it, leaving the minimap looking like it only ever drew
          // the visible viewport.
          "--xy-minimap-mask-background-color": hexToRgba(
            colors.foreground.primary,
            0.15,
          ),
          "--xy-controls-button-background-color": colors.background.primary,
          "--xy-controls-button-background-color-hover":
            colors.background.tertiary,
          "--xy-controls-button-color": colors.foreground.primary,
          "--xy-controls-button-border-color": colors.border.primary,
          "--xy-controls-box-shadow": shadows.level1,
        } as React.CSSProperties
      }
    >
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
          if (node.type === "lirGroup") {
            onLirGroupClick?.((node.data as { group: LirGroupNodeData }).group);
            return;
          }
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
          // Groups aren't scopes: no drill-down, no jump, nothing happens.
          if (node.type === "lirGroup") return;
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
            (node.data as { color?: string }).color ??
            colors.background.tertiary
          }
          // Node fills are light (operator colors and the cold end of the
          // heatmap gradient both skew pale) in light mode, and dark in
          // dark mode, so without an explicit stroke a scope of same-toned
          // nodes can render as an almost-blank minimap.
          nodeStrokeColor={colors.border.secondary}
        />
        {centerRef && <CenterHelper centerRef={centerRef} fitRef={fitRef} />}
        <ViewportGuard
          focusedScope={focusedScope}
          nodes={visible.nodes}
          positions={absolutePositions}
          paneRef={paneRef}
        />
      </ReactFlow>
    </Box>
  );
};
