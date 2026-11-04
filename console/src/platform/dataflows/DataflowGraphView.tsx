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
  useStore,
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
  type VisibleEdge,
  type VisibleGraph,
  type VisibleNode,
} from "./dataflowGraph";
import { NODE_DIMENSIONS, type Positions } from "./elkGraph";
import {
  LirGroupNode as LirGroupNodeComponent,
  OperatorNode,
  PortNode,
  RegionNode,
} from "./nodes";
import { hexToRgba, nodeFillColor } from "./nodeStyle";
import { useElkLayout } from "./useElkLayout";
import {
  revealCenter,
  shouldRefit,
  type ViewportFitState,
} from "./viewportFit";

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
  // The scope whose direct children this view renders. A region's chevron
  // gutter expands it in place, while rooting a view on it is done from
  // outside the canvas.
  focusedScope: NodeId;
  // Not read by the canvas: no node on it navigates into a scope.
  onNavigate: (scope: NodeId) => void;
  cacheKey: string;
  decorations?: GraphDecorations;
  selectedId?: string;
  activeMatchId?: string;
  // Edges connected to the clicked node, with endpoint labels resolved, so a
  // clicked port can show what flows through it without the caller
  // recomputing the (already filtered) visible graph.
  onNodeClick?: (node: VisibleNode, connectedEdges: SelectedEdge[]) => void;
  onEdgeClick?: (edge: SelectedEdge) => void;
  onPaneClick?: () => void;
  // A port's label jumps straight here when it has exactly one peer. With
  // several the label is still a link but selects instead, leaving the
  // choice to the detail panel; with none it isn't a link at all.
  onJumpToPeer?: (peer: PortPeer) => void;
  // Nodes to fit the viewport around (e.g. every operator belonging to one
  // LIR id, or a region and its children right after expanding it). Fitting
  // can't happen synchronously with the request: layout runs in a worker, so
  // these ids are retried across renders until their positions show up.
  fitOnIds?: string[] | null;
  onFit?: () => void;
  // Toggled from the toolbar; grouping is computed here (from the same
  // post-hideUnused node list elk actually lays out) rather than
  // by the caller, so a hidden node can never end up as a group's only
  // member.
  showLirGroups?: boolean;
  onLirGroupClick?: (group: LirGroupNodeData) => void;
  // The scopes expanded in place; drives nested node emission (via the
  // caller's deriveVisibleGraph) and the layout cache key.
  expandedScopes: Set<NodeId>;
  onToggleExpand: (id: NodeId) => void;
  // Pixels of the canvas's right edge covered by an overlay such as the
  // details panel. When positive, selecting a node that is not fully inside
  // the uncovered area pans it into view.
  occludedRight?: number;
}

// Pans (at constant zoom) to bring a newly selected node out from under an
// overlay. Keyed on the selection alone so that a data refresh, a layout
// change or the user panning away never moves the viewport again.
const SelectionReveal = ({
  selectedId,
  occludedRight,
}: {
  selectedId?: string;
  occludedRight: number;
}) => {
  const reactFlow = useReactFlow();
  const width = useStore((s) => s.width);
  const height = useStore((s) => s.height);
  // Read through a ref so the effect depends on the selection only.
  const latest = React.useRef({ reactFlow, width, height, occludedRight });
  React.useEffect(() => {
    latest.current = { reactFlow, width, height, occludedRight };
  });
  React.useEffect(() => {
    const current = latest.current;
    if (!selectedId || current.occludedRight <= 0) return;
    // An edge id (or a node whose layout has not landed) has no internal node.
    const internal = current.reactFlow.getInternalNode(selectedId);
    if (!internal) return;
    const viewport = current.reactFlow.getViewport();
    const center = revealCenter(
      {
        x: internal.internals.positionAbsolute.x,
        y: internal.internals.positionAbsolute.y,
        width: internal.measured.width ?? internal.width ?? 0,
        height: internal.measured.height ?? internal.height ?? 0,
      },
      viewport,
      { width: current.width, height: current.height },
      current.occludedRight,
    );
    if (!center) return;
    void current.reactFlow.setCenter(center.x, center.y, {
      zoom: viewport.zoom,
      duration: 200,
    });
  }, [selectedId]);
  return null;
};

// Fitting needs React Flow's context, which only exists below <ReactFlow>,
// while the requests to fit arrive as a prop on this component. This bridges
// the two: it renders nothing and only publishes the callback on a ref the
// component itself reads.
const FitHelper = ({
  fitRef,
}: {
  fitRef: React.MutableRefObject<((ids: string[]) => void) | null>;
}) => {
  const reactFlow = useReactFlow();
  React.useEffect(() => {
    // Assigning to the ref inside an effect is safe: it never runs during
    // render, so the ref just receives the latest fit callback.
    fitRef.current = (ids: string[]) => {
      void reactFlow.fitView({
        nodes: ids.map((id) => ({ id })),
        duration: 300,
        padding: 0.3,
        maxZoom: 1,
      });
    };
    return () => {
      fitRef.current = null;
    };
  }, [reactFlow, fitRef]);
  return null;
};

// Fitting needs React Flow's context, which only exists below <ReactFlow>,
// while the requests to fit arrive as a prop on this component. This renders
// nothing and checks shouldRefit once per landed layout (skipping the initial
// one, which React Flow's own `fitView` prop already covers). A pending
// targeted fit (a search or port jump into this scope) wins.
const ViewportGuard = ({
  fit,
  hasNodes,
  positions,
  targetedFitPending,
}: {
  fit: ViewportFitState;
  hasNodes: boolean;
  positions: Positions | null;
  targetedFitPending: boolean;
}) => {
  const reactFlow = useReactFlow();
  const lastRef = React.useRef<ViewportFitState>(fit);
  React.useEffect(() => {
    // The state is only recorded once positions for it have landed, so a
    // change that arrives while elk is still laying out is fit afterwards.
    if (!positions || !hasNodes) return;
    const prev = lastRef.current;
    lastRef.current = fit;
    if (!shouldRefit(prev, fit) || targetedFitPending) return;
    void reactFlow.fitView({ padding: 0.3, duration: 300 });
  }, [fit, hasNodes, positions, targetedFitPending, reactFlow]);
  return null;
};

// An expanded region and an LIR group render as an outline around their
// members, so the minimap must not paint them as a solid slab that hides the
// members drawn over it.
const isOutlineOnCanvas = (node: Node): boolean =>
  node.type === "lirGroup" ||
  (node.data as { expanded?: boolean }).expanded === true;

export const DataflowGraphView = ({
  visible: rawVisible,
  focusedScope,
  cacheKey,
  decorations,
  selectedId,
  activeMatchId,
  onNodeClick,
  onEdgeClick,
  onPaneClick,
  onJumpToPeer,
  fitOnIds,
  onFit,
  showLirGroups,
  onLirGroupClick,
  expandedScopes,
  onToggleExpand,
  occludedRight = 0,
}: DataflowGraphViewProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const visible = React.useMemo(() => {
    // A node is hidden only when every edge touching it is hidden too (see
    // computeDecorations), so plain removal never leaves a dangling edge and
    // never cuts a path that carried data.
    const hiddenNodeIds = decorations?.hiddenNodeIds;
    const hiddenEdgeIds = decorations?.hiddenEdgeIds;
    if (!hiddenNodeIds?.size && !hiddenEdgeIds?.size) return rawVisible;
    return {
      nodes: rawVisible.nodes.filter((n) => !hiddenNodeIds?.has(n.id)),
      edges: rawVisible.edges.filter((e) => !hiddenEdgeIds?.has(e.id)),
    };
  }, [rawVisible, decorations?.hiddenNodeIds, decorations?.hiddenEdgeIds]);

  // Grouping and in-place expansion are mutually exclusive: a LIR group can
  // straddle scope boundaries in ways the ELK container layout (which nests
  // by scope) can't represent, so any expansion turns grouping off regardless
  // of the toolbar toggle. This also protects a bookmarked ?expand= link from
  // restoring grouping and expansion together.
  const grouping = React.useMemo(
    () =>
      showLirGroups && expandedScopes.size === 0
        ? groupByLir(visible.nodes)
        : null,
    [showLirGroups, expandedScopes, visible.nodes],
  );

  const layoutKey = `${cacheKey}|${focusedScope}|${showLirGroups ?? false}|${
    decorations?.hiddenNodeIds
      ? [...decorations.hiddenNodeIds].sort().join(",")
      : ""
  }|${
    decorations?.hiddenEdgeIds
      ? [...decorations.hiddenEdgeIds].sort().join(",")
      : ""
  }|${[...expandedScopes].sort().join(",")}`;
  const fitState = React.useMemo(
    () => ({
      scope: focusedScope,
      nodeSet: visible.nodes
        .map((n) => n.id)
        .sort()
        .join(","),
      expansion: [...expandedScopes].sort().join(","),
    }),
    [focusedScope, visible.nodes, expandedScopes],
  );
  const { positions, layouting, error, retry } = useElkLayout(
    visible,
    layoutKey,
    grouping ?? undefined,
  );

  // Unified parent map for React Flow's parentId (member nodes below): LIR
  // grouping and scope nesting are mutually exclusive (see the grouping memo
  // above), but this merges them into one map anyway rather than picking a
  // source conditionally, so it stays correct regardless of which one is
  // populated.
  const parentOf = React.useMemo(() => {
    const m = new Map<string, string>(grouping?.parentOf ?? []);
    // Scope nesting is structural; it wins over the grouping overlay if both
    // ever populate an entry (they never do under the interlock).
    for (const n of visible.nodes) if (n.parentId) m.set(n.id, n.parentId);
    return m;
  }, [grouping, visible.nodes]);

  const fitRef = React.useRef<((ids: string[]) => void) | null>(null);

  React.useEffect(() => {
    if (!fitOnIds || fitOnIds.length === 0) return;
    // Fires as soon as any targets have a position rather than waiting for
    // every one, so a partially-expanded set (e.g. one member still hidden
    // behind a filter) doesn't block the fit indefinitely.
    const present = fitOnIds.filter((id) => positions?.[id]);
    if (present.length === 0) return;
    // FitHelper assigns fitRef.current in its own mount effect; on the rare
    // commit where that hasn't run yet, skip without marking the request
    // consumed (onFit stays uncalled) so the next positions or fitOnIds
    // change gets another chance, rather than silently dropping the fit.
    if (!fitRef.current) return;
    fitRef.current(present);
    onFit?.();
  }, [fitOnIds, positions, onFit]);

  const nodes: Node[] = React.useMemo(() => {
    if (!positions) return [];
    const groupParentOf = grouping?.parentOf;
    const groupNodes: Node[] = (grouping?.groups ?? []).map((g) => {
      const pos = positions[g.id] ?? { x: 0, y: 0, width: 0, height: 0 };
      return {
        id: g.id,
        type: "lirGroup",
        position: { x: pos.x, y: pos.y },
        parentId: groupParentOf?.get(g.id),
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
          color: colors.dataflow.lirGroup,
        },
      };
    });
    const memberNodes: Node[] = visible.nodes.map((n) => {
      const pos = positions[n.id] ?? { x: 0, y: 0, ...NODE_DIMENSIONS[n.kind] };
      return {
        id: n.id,
        type: n.kind,
        position: { x: pos.x, y: pos.y },
        parentId: parentOf.get(n.id),
        // Match the Left/Right handles so bezier control points meet the
        // node edges. Without this the edge curves toward the default
        // Bottom/Top and appears detached across region boundaries.
        targetPosition: Position.Left,
        sourcePosition: Position.Right,
        // Set as explicit node fields, not just CSS style: the MiniMap and
        // culled (onlyRenderVisibleElements) off-screen nodes both need a
        // known size without waiting for DOM measurement.
        width: pos.width,
        height: pos.height,
        style: {
          width: pos.width,
          height: pos.height,
          // An expanded region renders its children as its own React Flow
          // children (via parentId above) rather than drawing its own body,
          // so its wrapper must let clicks fall through to them, matching
          // the group-node click-through contract above.
          ...(n.kind === "region" && n.expanded
            ? { pointerEvents: "none" as const }
            : {}),
        },
        draggable: false,
        connectable: false,
        data: {
          node: n,
          dimmed: decorations?.dimmedNodeIds?.has(n.id) ?? false,
          color:
            decorations?.nodeColors?.get(n.id) ??
            nodeFillColor(n, colors.dataflow.operator, {
              arranged: colors.accent.purple,
              notArranged: colors.accent.green,
            }),
          selected: n.id === selectedId,
          activeMatch: n.id === activeMatchId,
          expanded: n.kind === "region" && (n.expanded ?? false),
          onToggleExpand,
          onJumpToPeer,
        },
      };
    });
    return [...groupNodes, ...memberNodes];
  }, [
    visible,
    positions,
    grouping,
    parentOf,
    decorations?.dimmedNodeIds,
    decorations?.nodeColors,
    selectedId,
    activeMatchId,
    colors,
    onToggleExpand,
    onJumpToPeer,
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
        // No gesture on this canvas carries meaning: every action has a
        // visible control (see nodes.tsx). Zooming on double-click would
        // therefore only ever fire by accident, while clicking through
        // several nodes in a row to inspect them.
        zoomOnDoubleClick={false}
        // Both default to 0, so any tremor during a click suppresses it.
        // The chevron gutter and the port links are small enough targets
        // that a shaky click would otherwise silently do nothing, while
        // still leaving a drag free to pan the canvas.
        paneClickDistance={4}
        nodeClickDistance={4}
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
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap
          pannable
          zoomable
          // The default 200x150 covers a third of a laptop canvas. The
          // right margin keeps it beside the details overlay instead of
          // under it, on top of React Flow's default 15px panel margin.
          style={{ width: 140, height: 90, marginRight: occludedRight + 15 }}
          nodeColor={(node) =>
            isOutlineOnCanvas(node)
              ? colors.background.tertiary
              : ((node.data as { color?: string }).color ??
                colors.background.tertiary)
          }
          // Node fills are light (operator colors and the cold end of the
          // heatmap gradient both skew pale) in light mode, and dark in
          // dark mode, so without an explicit stroke a scope of same-toned
          // nodes can render as an almost-blank minimap. A container drawn
          // as an outline on the canvas keeps its own color as the stroke.
          nodeStrokeColor={(node) =>
            isOutlineOnCanvas(node)
              ? ((node.data as { color?: string }).color ??
                colors.border.secondary)
              : colors.border.secondary
          }
        />
        <FitHelper fitRef={fitRef} />
        <SelectionReveal
          selectedId={selectedId}
          occludedRight={occludedRight}
        />
        <ViewportGuard
          fit={fitState}
          hasNodes={visible.nodes.length > 0}
          positions={positions}
          targetedFitPending={(fitOnIds?.length ?? 0) > 0}
        />
      </ReactFlow>
    </Box>
  );
};
