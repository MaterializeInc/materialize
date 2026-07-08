// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Badge, Box, HStack, Text, Tooltip } from "@chakra-ui/react";
import { Handle, type NodeProps, Position } from "@xyflow/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import type { VisibleNode } from "./dataflowGraph";
import {
  type FlowNodeData,
  formatElapsed,
  HIGHLIGHT_COLORS,
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

const CardShell = ({
  data,
  children,
}: {
  data: FlowNodeData;
  children: React.ReactNode;
}) => (
  <Box
    borderWidth="1px"
    borderRadius="md"
    px={2}
    py={1}
    width="100%"
    height="100%"
    overflow="hidden"
    background={data.color}
    opacity={data.dimmed ? 0.25 : 1}
    boxShadow={highlightShadow(data)}
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
        <Text fontSize="sm" fontWeight="600" noOfLines={1}>
          {data.node.label}
        </Text>
        {statLines(data.node).map((line) => (
          <Text key={line} fontSize="xs" noOfLines={1}>
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
      <Text fontSize="sm" fontWeight="600" noOfLines={1}>
        {data.node.label}
      </Text>
      <Badge fontSize="2xs" flexShrink={0}>
        {data.node.childCount}
      </Badge>
    </HStack>
    {statLines(data.node).map((line) => (
      <Text key={line} fontSize="xs" noOfLines={1}>
        {line}
      </Text>
    ))}
  </CardShell>
);

export const PortNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <Box
    borderWidth="1px"
    borderRadius="full"
    px={2}
    background="gray.100"
    opacity={data.dimmed ? 0.25 : 1}
    boxShadow={highlightShadow(data)}
  >
    <Text fontSize="2xs">{data.node.label}</Text>
    <Handle type="target" position={Position.Top} />
    <Handle type="source" position={Position.Bottom} />
  </Box>
);
