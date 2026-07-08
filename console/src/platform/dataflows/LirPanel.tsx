// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text } from "@chakra-ui/react";
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
  onHighlight,
  onSelect,
}: {
  structure: DataflowStructure;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (memberIds: NodeId[]) => void;
}) => {
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const tree = React.useMemo(() => lirTree(index), [index]);
  if (index.size === 0) return null;
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
              onHighlight={onHighlight}
              onSelect={onSelect}
            />
          ))}
        </Box>
      ))}
    </Box>
  );
};
