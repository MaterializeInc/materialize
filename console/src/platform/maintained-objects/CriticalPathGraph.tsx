// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Center,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { Canvas, GraphEdgeContainer, STROKE_WIDTH } from "~/components/Graph";
import { useDagreGraph } from "~/hooks/useDagreGraph";
import { MaterializeTheme } from "~/theme";

import { CriticalPathClusterMetrics } from "./CriticalPathClusterMetrics";
import { CriticalPathGraphNode } from "./CriticalPathGraphNode";
import {
  buildGraphView,
  buildSourceExportsView,
  GraphView,
} from "./criticalPathRenderable";
import {
  MaintainedObjectListItem,
  useCriticalPath,
  useObjectsLagHistory,
} from "./queries";
import { useSourceExports } from "./useSourceExports";

// The canvas centers the graph and pins its zoom controls to the bottom
// right, so a wide bottom row needs enough padding to clear them.
const CANVAS_PADDING = 40;
const WIDE_ROW_CANVAS_PADDING = 120;

const NODE_WIDTH = 160;
const NODE_HEIGHT = 56;
const RANK_SEP = 32;
const DEFAULT_VISIBLE_DEPTH = 2;

export interface CriticalPathGraphProps {
  probe: MaintainedObjectListItem;
  /** Null = live (pMAX over `lookbackMinutes`). */
  timestamp: Date | null;
  bucketSizeMs: number;
  lookbackMinutes: number;
}

const LoadingSpinner = () => (
  <Center height="200px" width="100%">
    <Spinner size="sm" />
  </Center>
);

/**
 * Walks upstream from the probe to its slowest inputs. A source has no
 * upstream inside Materialize, so for a source this draws the subsources and
 * tables it ingests into instead.
 */
export const CriticalPathGraph = (props: CriticalPathGraphProps) =>
  props.probe.objectType === "source" ? (
    <SourceExportsGraph {...props} />
  ) : (
    <UpstreamGraph {...props} />
  );

const UpstreamGraph = ({
  probe,
  timestamp,
  bucketSizeMs,
  lookbackMinutes,
}: CriticalPathGraphProps) => {
  const { data, isLoading, isError } = useCriticalPath({
    objectId: probe.id,
    timestamp,
    bucketSizeMs,
    lookbackMinutes,
  });
  const buildView = React.useCallback(
    (expandedBottleneckId: string | null, isExpanded: boolean) =>
      buildGraphView(
        data ?? { rows: [], directInputs: [] },
        probe,
        expandedBottleneckId,
        isExpanded ? Infinity : DEFAULT_VISIBLE_DEPTH,
      ),
    [data, probe],
  );

  if (isError) {
    return (
      <CriticalPathMessage>
        Failed to load the critical path.
      </CriticalPathMessage>
    );
  }
  if (isLoading || !data) return <LoadingSpinner />;
  if (data.rows.length === 0) {
    return <CriticalPathMessage>No upstream dependencies.</CriticalPathMessage>;
  }
  return (
    <CriticalPathGraphInner
      probe={probe}
      buildView={buildView}
      moreLabel={(count) => `+ ${count} more upstream paths`}
      timestamp={timestamp}
      bucketSizeMs={bucketSizeMs}
      lookbackMs={lookbackMinutes * 60 * 1000}
    />
  );
};

const SourceExportsGraph = ({
  probe,
  timestamp,
  bucketSizeMs,
  lookbackMinutes,
}: CriticalPathGraphProps) => {
  const exports = useSourceExports(probe.id);
  const { data, isLoading, isError } = useObjectsLagHistory({
    objectIds: [probe.id, ...exports.map((e) => e.id)],
    lookbackMs: lookbackMinutes * 60 * 1000,
  });
  const buildView = React.useCallback(
    (_expandedBottleneckId: string | null, isExpanded: boolean) =>
      buildSourceExportsView({
        probe,
        exports,
        readings: data ?? [],
        anchor: timestamp,
        bucketSizeMs,
        showAll: isExpanded,
      }),
    [probe, exports, data, timestamp, bucketSizeMs],
  );

  if (isLoading) return <LoadingSpinner />;
  if (isError) {
    return (
      <CriticalPathMessage>
        Failed to load the critical path.
      </CriticalPathMessage>
    );
  }
  // With no exports the graph is the source alone, which still carries the
  // cluster metrics below it.
  return (
    <CriticalPathGraphInner
      probe={probe}
      buildView={buildView}
      moreLabel={(count) => `+ ${count} more`}
      canvasPadding={WIDE_ROW_CANVAS_PADDING}
      timestamp={timestamp}
      bucketSizeMs={bucketSizeMs}
      lookbackMs={lookbackMinutes * 60 * 1000}
    />
  );
};

const CriticalPathMessage = ({ children }: React.PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Center height="120px" width="100%">
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {children}
      </Text>
    </Center>
  );
};

const CriticalPathGraphInner = ({
  probe,
  buildView,
  moreLabel,
  canvasPadding = CANVAS_PADDING,
  timestamp,
  bucketSizeMs,
  lookbackMs,
}: {
  probe: MaintainedObjectListItem;
  /** Builds the drawn graph; `isExpanded` reveals the nodes it hides. */
  buildView: (
    expandedBottleneckId: string | null,
    isExpanded: boolean,
  ) => GraphView;
  moreLabel: (hiddenCount: number) => string;
  canvasPadding?: number;
  timestamp: Date | null;
  bucketSizeMs: number;
  lookbackMs: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [expandedBottleneckId, setExpandedBottleneckId] = React.useState<
    string | null
  >(null);
  const [selectedNodeId, setSelectedNodeId] = React.useState<string | null>(
    null,
  );
  const [isExpanded, setIsExpanded] = React.useState(false);

  const { nodes, edges, hiddenChainCount } = React.useMemo(
    () => buildView(expandedBottleneckId, isExpanded),
    [buildView, expandedBottleneckId, isExpanded],
  );

  const dagreNodes = React.useMemo(
    () =>
      nodes.map((n) => ({
        id: n.id,
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
      })),
    [nodes],
  );

  const dagreEdges = React.useMemo(
    () => edges.map((r) => ({ parentId: r.id, childId: r.childId })),
    [edges],
  );

  const {
    graph,
    nodePositionMap,
    height,
    width,
    orderedGraphEdges,
    clampPoints,
  } = useDagreGraph({
    nodes: dagreNodes,
    edges: dagreEdges,
    selectedNodeId: undefined,
    ranksep: RANK_SEP,
  });

  if (!width || !height) return <LoadingSpinner />;

  const toggle = (id: string) => (curr: string | null) =>
    curr === id ? null : id;
  const displayedNode =
    nodes.find((n) => n.id === selectedNodeId) ??
    nodes.find((n) => n.isProbe) ??
    nodes[0];

  return (
    <VStack align="stretch" width="100%" spacing={4}>
      {(hiddenChainCount > 0 || isExpanded) && (
        <HStack justify="flex-end">
          <Text
            as="button"
            textStyle="text-small"
            color={colors.accent.brightPurple}
            cursor="pointer"
            _hover={{ textDecoration: "underline" }}
            onClick={() => setIsExpanded(!isExpanded)}
          >
            {isExpanded ? "Collapse" : moreLabel(hiddenChainCount)}
          </Text>
        </HStack>
      )}
      <Box
        position="relative"
        width="100%"
        height={`${height + canvasPadding}px`}
        overflow="hidden"
      >
        <Canvas width={width} height={height} selectedNode={null}>
          {nodes.map((node) => {
            const dagreNode = graph.node(node.id);
            const position = nodePositionMap.get(node.id);
            if (!dagreNode || !position) return null;
            return (
              <CriticalPathGraphNode
                key={node.id}
                node={node}
                pageObjectId={probe.id}
                expanded={expandedBottleneckId === node.id}
                selected={selectedNodeId === node.id}
                left={position.left}
                top={position.top}
                width={dagreNode.width}
                height={dagreNode.height}
                onToggleExpand={() => setExpandedBottleneckId(toggle(node.id))}
                onSelect={() => setSelectedNodeId(toggle(node.id))}
              />
            );
          })}
          <GraphEdgeContainer width={width} height={height}>
            {orderedGraphEdges.map((e) => {
              const dagreEdge = graph.edge(e);
              const parentPosition = nodePositionMap.get(e.v);
              if (!dagreEdge || !parentPosition) return null;
              const [first, ...rest] = clampPoints(dagreEdge.points);
              const points = [
                { x: first.x, y: parentPosition.bottom },
                ...rest,
              ];
              const isOffPath = !edges.find(
                (re) => re.id === e.v && re.childId === e.w,
              )?.isBottleneck;
              return (
                <CriticalPathEdgeLine
                  key={`${e.v}->${e.w}`}
                  points={points}
                  isOffPath={isOffPath}
                />
              );
            })}
          </GraphEdgeContainer>
        </Canvas>
      </Box>
      {displayedNode && (
        <CriticalPathClusterMetrics
          node={displayedNode}
          pageObjectId={probe.id}
          timestamp={timestamp}
          bucketSizeMs={bucketSizeMs}
          lookbackMs={lookbackMs}
        />
      )}
    </VStack>
  );
};

const CriticalPathEdgeLine = ({
  points,
  isOffPath,
}: {
  points: { x: number; y: number }[];
  isOffPath: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [start, ...rest] = points;
  const end = rest.pop();
  if (!end) return null;
  const controlPoints = rest.map((p) => `${p.x},${p.y}`).join(" ");
  return (
    <path
      d={`M${start.x},${start.y} S ${controlPoints} ${end.x},${end.y}`}
      stroke={isOffPath ? colors.border.secondary : colors.foreground.secondary}
      strokeWidth={STROKE_WIDTH}
      strokeDasharray={isOffPath ? "4 4" : undefined}
      fill="none"
    />
  );
};
