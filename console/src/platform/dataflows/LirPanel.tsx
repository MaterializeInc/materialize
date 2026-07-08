// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, HStack, Text } from "@chakra-ui/react";
import React from "react";

import {
  type DataflowStructure,
  lirIndex,
  lirTree,
  type LirTreeNode,
  type NodeId,
} from "./dataflowGraph";

const LirRow = ({
  node,
  onHighlight,
  onSelect,
}: {
  node: LirTreeNode;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (memberIds: NodeId[]) => void;
}) => (
  <>
    <Text
      fontSize="xs"
      pl={node.info.nesting * 3}
      cursor="pointer"
      _hover={{ background: "gray.100" }}
      onMouseEnter={() => onHighlight(new Set(node.memberIds))}
      onMouseLeave={() => onHighlight(null)}
      onClick={() => onSelect(node.memberIds)}
    >
      LIR {node.info.lirId}: {node.info.operator}
    </Text>
    {node.children.map((child) => (
      <LirRow
        key={child.key}
        node={child}
        onHighlight={onHighlight}
        onSelect={onSelect}
      />
    ))}
  </>
);

export const LirPanel = ({
  structure,
  collapsed,
  onToggleCollapsed,
  onHighlight,
  onSelect,
}: {
  structure: DataflowStructure;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (memberIds: NodeId[]) => void;
}) => {
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const tree = React.useMemo(
    () => lirTree(structure, index),
    [structure, index],
  );

  if (index.size === 0) return null;
  if (collapsed) {
    return (
      <Box
        width="24px"
        borderRightWidth="1px"
        flexShrink={0}
        display="flex"
        justifyContent="center"
        pt={2}
      >
        <Button
          size="2xs"
          variant="ghost"
          onClick={onToggleCollapsed}
          aria-label="Expand LIR panel"
        >
          »
        </Button>
      </Box>
    );
  }
  return (
    <Box
      width="280px"
      borderRightWidth="1px"
      p={3}
      overflowY="auto"
      flexShrink={0}
    >
      <HStack justifyContent="space-between" mb={2}>
        <Text fontSize="xs" fontWeight="600">
          LIR
        </Text>
        <Button
          size="2xs"
          variant="ghost"
          onClick={onToggleCollapsed}
          aria-label="Collapse LIR panel"
        >
          «
        </Button>
      </HStack>
      {[...tree.entries()].map(([exportId, roots]) => (
        <Box key={exportId} mb={3}>
          <Text fontSize="xs" color="gray.500">
            {exportId}
          </Text>
          {roots.map((root) => (
            <LirRow
              key={root.key}
              node={root}
              onHighlight={onHighlight}
              onSelect={onSelect}
            />
          ))}
        </Box>
      ))}
    </Box>
  );
};
