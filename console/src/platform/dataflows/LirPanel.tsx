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
  type LirInfo,
  type NodeId,
} from "./dataflowGraph";

export const LirPanel = ({
  structure,
  onHighlight,
}: {
  structure: DataflowStructure;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
}) => {
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const byExport = React.useMemo(() => {
    const groups = new Map<
      string,
      { key: string; info: LirInfo; memberIds: NodeId[] }[]
    >();
    for (const [key, entry] of index) {
      const list = groups.get(entry.info.exportId) ?? [];
      list.push({ key, ...entry });
      groups.set(entry.info.exportId, list);
    }
    return groups;
  }, [index]);
  if (index.size === 0) return null;
  return (
    <Box
      width="280px"
      borderRightWidth="1px"
      p={3}
      overflowY="auto"
      flexShrink={0}
    >
      {[...byExport.entries()].map(([exportId, entries]) => (
        <Box key={exportId} mb={3}>
          <Text fontSize="xs" color="gray.500">
            {exportId}
          </Text>
          {entries.map((e) => (
            <Text
              key={e.key}
              fontSize="xs"
              cursor="pointer"
              _hover={{ background: "gray.100" }}
              onMouseEnter={() => onHighlight(new Set(e.memberIds))}
              onMouseLeave={() => onHighlight(null)}
            >
              LIR {e.info.lirId}: {e.info.operator}
            </Text>
          ))}
        </Box>
      ))}
    </Box>
  );
};
