// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text } from "@chakra-ui/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import {
  type DataflowStructure,
  formatElapsedNs,
  lirIndex,
  lirTree,
  type LirTreeNode,
  type NodeId,
} from "./dataflowGraph";

const LirRow = ({
  node,
  pinnedKeys,
  onHighlight,
  onSelect,
  onTogglePin,
}: {
  node: LirTreeNode;
  pinnedKeys: ReadonlySet<string>;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (memberIds: NodeId[]) => void;
  onTogglePin: (key: string, memberIds: NodeId[]) => void;
}) => (
  <>
    <Text
      fontSize="xs"
      fontWeight={pinnedKeys.has(node.key) ? "700" : "400"}
      pl={node.info.nesting * 3}
      cursor="pointer"
      _hover={{ background: "gray.100" }}
      onMouseEnter={() => onHighlight(new Set(node.memberIds))}
      onMouseLeave={() => onHighlight(null)}
      onClick={() => onSelect(node.memberIds)}
      onDoubleClick={() => onTogglePin(node.key, node.memberIds)}
    >
      LIR {node.info.lirId}: {node.info.operator}
    </Text>
    {node.children.map((child) => (
      <LirRow
        key={child.key}
        node={child}
        pinnedKeys={pinnedKeys}
        onHighlight={onHighlight}
        onSelect={onSelect}
        onTogglePin={onTogglePin}
      />
    ))}
  </>
);

const LirSummaryCard = ({ node }: { node: LirTreeNode }) => (
  <Box borderWidth="1px" borderRadius="md" p={2} mb={2}>
    <Text fontSize="xs" fontWeight="600" noOfLines={2}>
      LIR {node.info.lirId}: {node.info.operator}
    </Text>
    <HStack justifyContent="space-between">
      <Text fontSize="2xs" color="gray.500">
        Records
      </Text>
      <Text fontSize="2xs" textStyle="monospace">
        {node.summary.arrangementRecords.toString()}
      </Text>
    </HStack>
    <HStack justifyContent="space-between">
      <Text fontSize="2xs" color="gray.500">
        Memory
      </Text>
      <Text fontSize="2xs" textStyle="monospace">
        {formatBytesShort(node.summary.arrangementSize)}
      </Text>
    </HStack>
    <HStack justifyContent="space-between">
      <Text fontSize="2xs" color="gray.500">
        Elapsed
      </Text>
      <Text fontSize="2xs" textStyle="monospace">
        {formatElapsedNs(node.summary.elapsedNs)}
      </Text>
    </HStack>
  </Box>
);

export const LirPanel = ({
  structure,
  pinnedKeys,
  onHighlight,
  onSelect,
  onTogglePin,
}: {
  structure: DataflowStructure;
  // Lives in the parent: dimming decorations need the union of every pinned
  // LIR id's members, computed alongside hover, so the pin state can't live
  // locally here.
  pinnedKeys: ReadonlySet<string>;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (memberIds: NodeId[]) => void;
  onTogglePin: (key: string, memberIds: NodeId[]) => void;
}) => {
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const tree = React.useMemo(
    () => lirTree(structure, index),
    [structure, index],
  );
  const byKey = React.useMemo(() => {
    const map = new Map<string, LirTreeNode>();
    const walk = (node: LirTreeNode) => {
      map.set(node.key, node);
      node.children.forEach(walk);
    };
    for (const roots of tree.values()) roots.forEach(walk);
    return map;
  }, [tree]);

  if (index.size === 0) return null;
  const pinnedNodes = [...pinnedKeys]
    .map((key) => byKey.get(key))
    .filter((n) => n !== undefined);
  return (
    <Box
      width="280px"
      borderRightWidth="1px"
      p={3}
      overflowY="auto"
      flexShrink={0}
    >
      {[...tree.entries()].map(([exportId, roots]) => (
        <Box key={exportId} mb={3}>
          <Text fontSize="xs" color="gray.500">
            {exportId}
          </Text>
          {roots.map((root) => (
            <LirRow
              key={root.key}
              node={root}
              pinnedKeys={pinnedKeys}
              onHighlight={onHighlight}
              onSelect={onSelect}
              onTogglePin={onTogglePin}
            />
          ))}
        </Box>
      ))}
      {pinnedNodes.length > 0 && (
        <Box borderTopWidth="1px" pt={2} mt={2}>
          <Text fontSize="xs" fontWeight="600" mb={2}>
            Pinned
          </Text>
          {pinnedNodes.map((node) => (
            <LirSummaryCard key={node.key} node={node} />
          ))}
        </Box>
      )}
    </Box>
  );
};
