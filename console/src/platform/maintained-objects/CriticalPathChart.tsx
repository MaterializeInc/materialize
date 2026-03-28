// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useTheme, VStack } from "@chakra-ui/react";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleBand, scaleLinear } from "@visx/scale";
import { BarRounded } from "@visx/shape";
import { Group } from "@visx/group";
import React from "react";

import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import { CriticalPathEdge } from "~/api/materialize/maintained-objects/objectDependencies";
import { MaterializeTheme } from "~/theme";

interface WaterfallNode {
  id: string;
  name: string;
  type: string;
  /** Self delay: min(input frontiers) - output frontier. Positive = this node is the bottleneck */
  selfDelay: number;
  /** Frontier lag: distance from wallclock time */
  frontierLag: number;
}

/**
 * Converts edges to a topologically sorted list of nodes with self-delay.
 *
 * Self-delay = min(input frontiers) - output frontier.
 * Positive self-delay means THIS node is the bottleneck (it's behind its inputs).
 * Zero self-delay means the delay is upstream, not at this node.
 *
 * This matches Frank McSherry's prototype: the bar shows WHERE in the chain
 * the lag is being introduced, not just the total gap.
 */
function edgesToWaterfallNodes(
  edges: CriticalPathEdge[],
  probeId: string,
): WaterfallNode[] {
  const nodeMap = new Map<string, WaterfallNode>();
  const childToParents = new Map<string, Set<string>>();

  for (const edge of edges) {
    // Source node: we get its frontier lag from the edge data
    if (!nodeMap.has(edge.sourceId)) {
      nodeMap.set(edge.sourceId, {
        id: edge.sourceId,
        name: edge.sourceName,
        type: edge.sourceType,
        selfDelay: 0, // Sources (root nodes) have 0 self-delay by definition
        frontierLag: edge.sourceFrontierLag,
      });
    }

    // Target node: use targetSelfDelay from the query
    if (!nodeMap.has(edge.targetId)) {
      nodeMap.set(edge.targetId, {
        id: edge.targetId,
        name: edge.targetName,
        type: edge.targetType,
        selfDelay: Math.max(edge.targetSelfDelay, 0),
        frontierLag: edge.targetFrontierLag,
      });
    } else {
      // Update with max self-delay if seen from multiple edges
      const existing = nodeMap.get(edge.targetId)!;
      existing.selfDelay = Math.max(existing.selfDelay, edge.targetSelfDelay);
    }

    if (!childToParents.has(edge.targetId)) {
      childToParents.set(edge.targetId, new Set());
    }
    childToParents.get(edge.targetId)!.add(edge.sourceId);
  }

  // Topological sort: roots first, probe last
  const sorted: WaterfallNode[] = [];
  const visited = new Set<string>();

  const visit = (id: string) => {
    if (visited.has(id)) return;
    visited.add(id);
    const parents = childToParents.get(id);
    if (parents) {
      for (const parentId of parents) {
        visit(parentId);
      }
    }
    const node = nodeMap.get(id);
    if (node) sorted.push(node);
  };

  visit(probeId);
  for (const id of nodeMap.keys()) {
    visit(id);
  }

  return sorted;
}

export interface CriticalPathChartProps {
  edges: CriticalPathEdge[];
  probeId: string;
  onObjectClick: (objectId: string) => void;
}

const MARGIN = { top: 8, right: 60, bottom: 8, left: 200 };
const BAR_HEIGHT = 28;
const ROW_HEIGHT = 40;
const DOT_RADIUS = 5;

export const CriticalPathChart = ({
  edges,
  probeId,
  onObjectClick,
}: CriticalPathChartProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const nodes = React.useMemo(
    () => edgesToWaterfallNodes(edges, probeId),
    [edges, probeId],
  );

  const chartHeight = nodes.length * ROW_HEIGHT + MARGIN.top + MARGIN.bottom;

  return (
    <VStack align="start" spacing={3} width="100%">
      <Text textStyle="text-small-heavy">
        Full critical path to sources
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Data flows top to bottom. Bar width shows incremental delay introduced at each step.
      </Text>

      <Box
        width="100%"
        borderRadius="lg"
        border="1px"
        borderColor={colors.border.primary}
        bg={colors.background.primary}
        overflow="hidden"
      >
        <ParentSize debounceTime={100}>
          {({ width }) => {
            if (width < 100) return null;
            return (
              <CriticalPathSVG
                nodes={nodes}
                probeId={probeId}
                width={width}
                height={chartHeight}
                onObjectClick={onObjectClick}
              />
            );
          }}
        </ParentSize>
      </Box>
    </VStack>
  );
};

const CriticalPathSVG = ({
  nodes,
  probeId,
  width,
  height,
  onObjectClick,
}: {
  nodes: WaterfallNode[];
  probeId: string;
  width: number;
  height: number;
  onObjectClick: (objectId: string) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const maxDelay = Math.max(...nodes.map((n) => n.selfDelay), 1);

  const yScale = scaleBand({
    domain: nodes.map((n) => n.id),
    range: [MARGIN.top, height - MARGIN.bottom],
    padding: 0.2,
  });

  const xScale = scaleLinear({
    domain: [0, maxDelay],
    range: [MARGIN.left, width - MARGIN.right],
    nice: true,
  });

  const getBarColor = (delaySeconds: number) => {
    if (delaySeconds >= OUTDATED_THRESHOLD_SECONDS) return colors.accent.red;
    if (delaySeconds >= OUTDATED_THRESHOLD_SECONDS / 2) return colors.accent.orange;
    if (delaySeconds > 0) return colors.accent.brightPurple;
    return colors.accent.green;
  };

  const getDotColor = (delaySeconds: number, isProbe: boolean) => {
    if (isProbe && delaySeconds <= 0) return colors.foreground.secondary;
    if (delaySeconds >= OUTDATED_THRESHOLD_SECONDS) return colors.accent.red;
    if (delaySeconds >= OUTDATED_THRESHOLD_SECONDS / 2) return colors.accent.orange;
    return colors.accent.brightPurple;
  };

  return (
    <svg width={width} height={height}>
      {/* Background grid lines */}
      {xScale.ticks(4).map((tick) => (
        <line
          key={tick}
          x1={xScale(tick)}
          x2={xScale(tick)}
          y1={MARGIN.top}
          y2={height - MARGIN.bottom}
          stroke={colors.border.primary}
          strokeDasharray="4"
          opacity={0.5}
        />
      ))}

      {nodes.map((node) => {
        const isProbe = node.id === probeId;
        const delaySeconds = node.selfDelay / 1000;
        const y = yScale(node.id) ?? 0;
        const barY = y + (yScale.bandwidth() - BAR_HEIGHT) / 2;
        const barWidth = Math.max(
          xScale(node.selfDelay) - xScale(0),
          4,
        );

        return (
          <Group
            key={node.id}
            style={{ cursor: isProbe ? "default" : "pointer" }}
            onClick={!isProbe ? () => onObjectClick(node.id) : undefined}
          >
            {/* Hover background */}
            <rect
              x={0}
              y={y}
              width={width}
              height={yScale.bandwidth()}
              fill="transparent"
              rx={4}
            >
              <set
                attributeName="fill"
                to={colors.background.secondary}
                begin="mouseover"
                end="mouseout"
              />
            </rect>

            {/* Dot indicator */}
            <circle
              cx={MARGIN.left - 16}
              cy={barY + BAR_HEIGHT / 2}
              r={DOT_RADIUS}
              fill={getDotColor(delaySeconds, isProbe)}
            />

            {/* Name label */}
            <text
              x={MARGIN.left - 28}
              y={barY + BAR_HEIGHT / 2 - 4}
              textAnchor="end"
              fontSize="12px"
              fontWeight={500}
              fill={
                isProbe
                  ? colors.foreground.primary
                  : colors.accent.brightPurple
              }
              style={{ fontFamily: "inherit" }}
            >
              {node.name.length > 22
                ? node.name.slice(0, 22) + "…"
                : node.name}
            </text>

            {/* Type sublabel */}
            <text
              x={MARGIN.left - 28}
              y={barY + BAR_HEIGHT / 2 + 10}
              textAnchor="end"
              fontSize="10px"
              fill={colors.foreground.secondary}
              style={{ fontFamily: "inherit" }}
            >
              {node.type}
            </text>

            {/* Bar */}
            <BarRounded
              x={xScale(0)}
              y={barY}
              width={barWidth}
              height={BAR_HEIGHT}
              fill={getBarColor(delaySeconds)}
              radius={4}
              all
            />

            {/* Self delay label */}
            <text
              x={xScale(0) + barWidth + 8}
              y={barY + BAR_HEIGHT / 2 - 2}
              fontSize="11px"
              fontWeight={600}
              fill={
                delaySeconds >= OUTDATED_THRESHOLD_SECONDS
                  ? colors.accent.red
                  : delaySeconds >= OUTDATED_THRESHOLD_SECONDS / 2
                    ? colors.accent.orange
                    : colors.foreground.secondary
              }
              style={{ fontFamily: "inherit" }}
            >
              {delaySeconds > 0
                ? `self: +${delaySeconds.toFixed(1)}s`
                : "self: 0s"}
            </text>
            {/* Frontier lag label */}
            <text
              x={xScale(0) + barWidth + 8}
              y={barY + BAR_HEIGHT / 2 + 12}
              fontSize="10px"
              fill={colors.foreground.secondary}
              style={{ fontFamily: "inherit" }}
            >
              {`lag: ${(node.frontierLag / 1000).toFixed(1)}s`}
            </text>
          </Group>
        );
      })}
    </svg>
  );
};

export default CriticalPathChart;
