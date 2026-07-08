// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Badge, Box, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import { Handle, type NodeProps, Position } from "@xyflow/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { formatBytesShort } from "~/utils/format";

import type { VisibleNode } from "./dataflowGraph";
import {
  type FlowGroupData,
  type FlowNodeData,
  formatElapsed,
  HIGHLIGHT_COLORS,
  textColorFor,
} from "./nodeStyle";

// Records and size share one line, not two: the fixed node height only has
// room for name + a couple of stat lines before text spills out of the box.
const statLines = (node: VisibleNode) => {
  const lines: string[] = [];
  const stats = node.stats;
  if (!stats) return lines;
  if (stats.arrangementRecords > 0n) {
    lines.push(
      `${stats.arrangementRecords} rec · ${formatBytesShort(stats.arrangementSize)}`,
    );
  }
  if (stats.elapsedNs > 0n) lines.push(formatElapsed(stats.elapsedNs));
  return lines;
};

// Selection (clicked) wins over the active search match when both apply to
// the same node; both are visually distinct from ordinary dimming.
const highlightShadow = (data: FlowNodeData): string | undefined => {
  if (data.selected) return `0 0 0 2px ${HIGHLIGHT_COLORS.selected}`;
  if (data.activeMatch) return `0 0 0 2px ${HIGHLIGHT_COLORS.activeMatch}`;
  return undefined;
};

// A subtle resting shadow (matching the app's other node-graph view) so
// cards read as raised surfaces even when unselected; highlightShadow
// overrides it with a colored ring for selected/matched nodes.
const RESTING_SHADOW = "0px 0.5px 2.5px 0 rgba(0, 0, 0, 0.08)";

const CardShell = ({
  data,
  children,
}: {
  data: FlowNodeData;
  children: React.ReactNode;
}) => (
  <Box
    borderWidth="1px"
    borderRadius="8px"
    px={2}
    py={1}
    width="100%"
    height="100%"
    overflow="hidden"
    background={data.color}
    color={textColorFor(data.color)}
    opacity={data.dimmed ? 0.25 : 1}
    boxShadow={highlightShadow(data) ?? RESTING_SHADOW}
  >
    {children}
    <Handle type="target" position={Position.Top} />
    <Handle type="source" position={Position.Bottom} />
  </Box>
);

export const OperatorNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <CardShell data={data}>
    <Tooltip
      label={data.node.lir
        .map((l) => `LIR ${l.lirId}: ${l.operator}`)
        .join(", ")}
      isDisabled={data.node.lir.length === 0}
    >
      <Box>
        <Text textStyle="text-ui-med" noOfLines={1}>
          {data.node.label}
        </Text>
        {statLines(data.node).map((line) => (
          <Text key={line} textStyle="text-small" noOfLines={1}>
            {line}
          </Text>
        ))}
      </Box>
    </Tooltip>
  </CardShell>
);

// Always shown collapsed (a scope is never expanded in place); double-click
// navigates into it instead of unfolding it here.
export const RegionNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <CardShell data={data}>
    <HStack justifyContent="space-between" spacing={1}>
      <Text textStyle="text-ui-med" noOfLines={1}>
        {data.node.label}
      </Text>
      <Badge fontSize="2xs" flexShrink={0}>
        {data.node.childCount}
      </Badge>
    </HStack>
    {statLines(data.node).map((line) => (
      <Text key={line} textStyle="text-small" noOfLines={1}>
        {line}
      </Text>
    ))}
  </CardShell>
);

export const PortNode = ({ data }: NodeProps & { data: FlowNodeData }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      borderWidth="1px"
      borderRadius="full"
      px={2}
      background={colors.background.tertiary}
      opacity={data.dimmed ? 0.25 : 1}
      boxShadow={highlightShadow(data)}
    >
      <Text fontSize="2xs">{data.node.label}</Text>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </Box>
  );
};

// A label-only wrapper around its members. Header is clickable
// (pointerEvents="auto"), body is not (pointerEvents="none"). For clicks on
// the body to pass through to underlying members, the node object this
// component is instantiated with must also set style={{pointerEvents:"none"}},
// since React Flow's own node wrapper defaults to pointer-events:all and
// this component's root cannot override an ancestor. Its width/height come
// from elk's auto-sized bounds, same as every other node in this file.
export const LirGroupNode = ({ data }: NodeProps & { data: FlowGroupData }) => (
  <Box width="100%" height="100%" position="relative" pointerEvents="none">
    <Box
      position="absolute"
      inset={0}
      borderWidth="2px"
      borderStyle="dashed"
      borderRadius="md"
      borderColor={data.color}
    />
    <Box
      position="absolute"
      top={0}
      left={0}
      px={2}
      py="1px"
      pointerEvents="auto"
      cursor="pointer"
      background={data.color}
      borderBottomRightRadius="md"
    >
      <Text fontSize="2xs" color={textColorFor(data.color)} noOfLines={1}>
        {data.label}
      </Text>
    </Box>
  </Box>
);
