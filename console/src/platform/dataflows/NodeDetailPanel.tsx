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
import type { SelectedEdge } from "./DataflowGraphView";

export type Selection =
  | { kind: "node"; node: VisibleNode; connectedEdges?: SelectedEdge[] }
  | { kind: "edge"; edge: SelectedEdge };

const Row = ({ label, value }: { label: string; value: string }) => (
  <HStack justifyContent="space-between">
    <Text fontSize="xs" color="gray.500">
      {label}
    </Text>
    <Text fontSize="xs" textStyle="monospace">
      {value}
    </Text>
  </HStack>
);

// Channel types are raw Rust container type signatures (e.g. nested Vec<...>
// generics) with no spaces to wrap on, so they get their own full-width
// stacked row instead of Row's space-between line.
const TypeRow = ({ channelTypes }: { channelTypes: string[] }) => (
  <Box>
    <Text fontSize="xs" color="gray.500">
      Type
    </Text>
    <Text fontSize="xs" textStyle="monospace" wordBreak="break-all">
      {channelTypes.join(", ") || "unknown"}
    </Text>
  </Box>
);

const EdgeRows = ({ edge }: { edge: SelectedEdge }) => (
  <>
    <Row label="Records" value={edge.messagesSent.toString()} />
    <Row label="Batches" value={edge.batchesSent.toString()} />
    <TypeRow channelTypes={edge.channelTypes} />
  </>
);

const NodeDetail = ({
  node,
  connectedEdges,
}: {
  node: VisibleNode;
  connectedEdges?: SelectedEdge[];
}) => (
  <>
    <Row label="Kind" value={node.kind} />
    {node.address && <Row label="Address" value={node.address.join(".")} />}
    {node.operatorId !== null && (
      <Row label="Operator ID" value={node.operatorId.toString()} />
    )}
    {node.lir.length > 0 && (
      <Row
        label="LIR ID"
        value={[...new Set(node.lir.map((l) => l.lirId))].join(", ")}
      />
    )}
    {node.childCount > 0 && (
      <Row label="Children" value={String(node.childCount)} />
    )}
    {node.stats && (
      <>
        <Row
          label="Arranged records"
          value={node.stats.arrangementRecords.toString()}
        />
        <Row
          label="Arrangement size"
          value={formatBytesShort(node.stats.arrangementSize)}
        />
        <Row
          label="Elapsed"
          value={`${Math.round(Number(node.stats.elapsedNs) / 1e9)}s`}
        />
      </>
    )}
    {node.transitive && node.childCount > 0 && (
      <>
        <Row
          label="Subtree records"
          value={node.transitive.arrangementRecords.toString()}
        />
        <Row
          label="Subtree size"
          value={formatBytesShort(node.transitive.arrangementSize)}
        />
        <Row
          label="Subtree elapsed"
          value={`${Math.round(Number(node.transitive.elapsedNs) / 1e9)}s`}
        />
      </>
    )}
    {node.lir.map((l) => (
      <Text key={`${l.exportId}/${l.lirId}`} fontSize="xs" mt={2}>
        LIR {l.lirId} ({l.exportId}): {l.operator}
      </Text>
    ))}
    {connectedEdges && connectedEdges.length > 0 && (
      <Box mt={2}>
        <Text fontSize="xs" fontWeight="600" mb={1}>
          Connected edges
        </Text>
        {connectedEdges.map((e) => (
          <Box key={e.id} mb={2}>
            <Text fontSize="2xs" color="gray.500" noOfLines={1}>
              {e.sourceLabel} → {e.targetLabel}
            </Text>
            <EdgeRows edge={e} />
          </Box>
        ))}
      </Box>
    )}
  </>
);

export const NodeDetailPanel = ({
  selection,
  onClose,
}: {
  selection: Selection;
  onClose: () => void;
}) => {
  const title =
    selection.kind === "node"
      ? selection.node.label
      : `${selection.edge.sourceLabel} → ${selection.edge.targetLabel}`;
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
          {title}
        </Text>
        <CloseButton onClick={onClose} />
      </HStack>
      {selection.kind === "node" ? (
        <NodeDetail
          node={selection.node}
          connectedEdges={selection.connectedEdges}
        />
      ) : (
        <EdgeRows edge={selection.edge} />
      )}
    </Box>
  );
};
