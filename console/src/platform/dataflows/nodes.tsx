// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Badge, Box, Text, Tooltip } from "@chakra-ui/react";
import { Handle, type NodeProps, Position } from "@xyflow/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import type { VisibleNode } from "./dataflowGraph";
import { type FlowNodeData, formatElapsed } from "./nodeStyle";

const statLines = (node: VisibleNode) => {
  const lines: string[] = [];
  const stats = node.stats;
  if (!stats) return lines;
  if (stats.arrangementRecords > 0n) {
    lines.push(`${stats.arrangementRecords} arranged records`);
    lines.push(formatBytesShort(stats.arrangementSize));
  }
  if (stats.elapsedNs > 0n) lines.push(formatElapsed(stats.elapsedNs));
  return lines;
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
  >
    {children}
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
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

export const CollapsedRegionNode = ({
  data,
}: NodeProps & { data: FlowNodeData }) => (
  <CardShell data={data}>
    <Text fontSize="sm" fontWeight="600" noOfLines={1}>
      {data.node.label}
    </Text>
    <Badge fontSize="2xs">{data.node.childCount} children</Badge>
    {statLines(data.node).map((line) => (
      <Text key={line} fontSize="xs" noOfLines={1}>
        {line}
      </Text>
    ))}
  </CardShell>
);

export const RegionNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <Box
    borderWidth="2px"
    borderStyle="dashed"
    borderRadius="md"
    width="100%"
    height="100%"
    background={`${data.color}20`}
    opacity={data.dimmed ? 0.25 : 1}
  >
    <Text fontSize="sm" fontWeight="600" px={2} noOfLines={1}>
      {data.node.label}
    </Text>
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
  </Box>
);

export const PortNode = ({ data }: NodeProps & { data: FlowNodeData }) => (
  <Box
    borderWidth="1px"
    borderRadius="full"
    px={2}
    background="gray.100"
    opacity={data.dimmed ? 0.25 : 1}
  >
    <Text fontSize="2xs">{data.node.label}</Text>
    <Handle type="target" position={Position.Left} />
    <Handle type="source" position={Position.Right} />
  </Box>
);
