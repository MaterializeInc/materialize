// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, CloseButton, HStack, Text } from "@chakra-ui/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import type { VisibleNode } from "./dataflowGraph";

export const NodeDetailPanel = ({
  node,
  onClose,
}: {
  node: VisibleNode;
  onClose: () => void;
}) => {
  const row = (label: string, value: string) => (
    <HStack key={label} justifyContent="space-between">
      <Text fontSize="xs" color="gray.500">
        {label}
      </Text>
      <Text fontSize="xs" textStyle="monospace">
        {value}
      </Text>
    </HStack>
  );
  return (
    <Box
      width="320px"
      borderLeftWidth="1px"
      p={3}
      overflowY="auto"
      flexShrink={0}
    >
      <HStack justifyContent="space-between">
        <Text fontWeight="600" noOfLines={2}>
          {node.label}
        </Text>
        <CloseButton onClick={onClose} />
      </HStack>
      {row("Kind", node.kind)}
      {node.address && row("Address", node.address.join("."))}
      {node.childCount > 0 && row("Children", String(node.childCount))}
      {node.stats && (
        <>
          {row("Arranged records", node.stats.arrangementRecords.toString())}
          {row(
            "Arrangement size",
            formatBytesShort(node.stats.arrangementSize),
          )}
          {row("Elapsed", `${Math.round(Number(node.stats.elapsedNs) / 1e9)}s`)}
        </>
      )}
      {node.transitive && node.childCount > 0 && (
        <>
          {row(
            "Subtree records",
            node.transitive.arrangementRecords.toString(),
          )}
          {row(
            "Subtree size",
            formatBytesShort(node.transitive.arrangementSize),
          )}
          {row(
            "Subtree elapsed",
            `${Math.round(Number(node.transitive.elapsedNs) / 1e9)}s`,
          )}
        </>
      )}
      {node.lir.map((l) => (
        <Text key={`${l.exportId}/${l.lirId}`} fontSize="xs" mt={2}>
          LIR {l.lirId} ({l.exportId}): {l.operator}
        </Text>
      ))}
    </Box>
  );
};
