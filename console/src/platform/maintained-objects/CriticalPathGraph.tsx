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
  Button,
  Center,
  HStack,
  Spinner,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { Canvas, GraphEdgeContainer, STROKE_WIDTH } from "~/components/Graph";
import { useDagreGraph } from "~/hooks/useDagreGraph";
import { MaterializeTheme } from "~/theme";

import { CriticalPathGraphNode } from "./CriticalPathGraphNode";
import { buildGraphView } from "./criticalPathRenderable";
import {
  CriticalPathData,
  MaintainedObjectListItem,
  useCriticalPath,
} from "./queries";

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

export const CriticalPathGraph = ({
  probe,
  timestamp,
  bucketSizeMs,
  lookbackMinutes,
}: CriticalPathGraphProps) => {
  const { data, isLoading } = useCriticalPath({
    objectId: probe.id,
    timestamp,
    bucketSizeMs,
    lookbackMinutes,
  });

  if (isLoading || !data) return <LoadingSpinner />;
  return <CriticalPathGraphInner probe={probe} data={data} />;
};

const CriticalPathGraphInner = ({
  probe,
  data,
}: {
  probe: MaintainedObjectListItem;
  data: CriticalPathData;
}) => {
  const [expandedBottleneckId, setExpandedBottleneckId] = React.useState<
    string | null
  >(null);
  const [selectedNodeId, setSelectedNodeId] = React.useState<string | null>(
    null,
  );
  const [visibleDepth, setVisibleDepth] = React.useState(DEFAULT_VISIBLE_DEPTH);

  const { nodes, edges, hiddenChainCount } = React.useMemo(
    () => buildGraphView(data, probe, expandedBottleneckId, visibleDepth),
    [data, probe, expandedBottleneckId, visibleDepth],
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

  const isExpanded = visibleDepth > DEFAULT_VISIBLE_DEPTH;
  const toggle = (id: string) => (curr: string | null) =>
    curr === id ? null : id;

  return (
    <VStack align="stretch" width="100%" spacing={2}>
      {(hiddenChainCount > 0 || isExpanded) && (
        <HStack justify="flex-end">
          <Button
            size="sm"
            variant="ghost"
            onClick={() =>
              setVisibleDepth(isExpanded ? DEFAULT_VISIBLE_DEPTH : Infinity)
            }
          >
            {isExpanded ? "Collapse" : `Show ${hiddenChainCount} more upstream`}
          </Button>
        </HStack>
      )}
      <Box
        position="relative"
        width="100%"
        height={`${height + 40}px`}
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
              const points = [{ x: first.x, y: parentPosition.bottom }, ...rest];
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
