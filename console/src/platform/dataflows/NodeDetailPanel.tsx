// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, CloseButton, HStack, Text } from "@chakra-ui/react";
import React from "react";

import { formatBytesShort } from "~/utils/format";

import {
  formatElapsedNs,
  type LirTreeNode,
  type PortPeer,
  type VisibleNode,
} from "./dataflowGraph";
import type { SelectedEdge } from "./DataflowGraphView";
import { LirSummaryCard } from "./LirPanel";
import { prettyPrintChannelType } from "./nodeStyle";

export type Selection =
  | { kind: "node"; node: VisibleNode; connectedEdges?: SelectedEdge[] }
  | { kind: "edge"; edge: SelectedEdge }
  | { kind: "lirGroup"; node: LirTreeNode };

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

// Channel types are Rust container type signatures (pretty-printed, but
// still nested-generic shaped, e.g. "[Rc<OrdValBatch<...>>]") long enough
// that they get their own full-width stacked row instead of Row's
// space-between line.
const TypeRow = ({ channelTypes }: { channelTypes: string[] }) => (
  <Box>
    <Text fontSize="xs" color="gray.500">
      Type
    </Text>
    <Text fontSize="xs" textStyle="monospace" wordBreak="break-all">
      {channelTypes.map(prettyPrintChannelType).join(", ") || "unknown"}
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

// A port's peer lives outside the current view (that's why it's a port and
// not a drawn edge), so its only affordance is a jump: navigate to wherever
// the peer actually is instead of tracing a line to it.
const PeerRow = ({
  peer,
  onJumpTo,
}: {
  peer: PortPeer;
  onJumpTo: (peer: PortPeer) => void;
}) => (
  <Box mb={2}>
    <HStack justifyContent="space-between">
      <Text fontSize="2xs" color="gray.500" noOfLines={1}>
        → {peer.label}
      </Text>
      <Button size="2xs" variant="outline" onClick={() => onJumpTo(peer)}>
        Jump
      </Button>
    </HStack>
    <Row label="Records" value={peer.messagesSent.toString()} />
    <Row label="Batches" value={peer.batchesSent.toString()} />
    <TypeRow channelTypes={peer.channelTypes} />
  </Box>
);

const NodeDetail = ({
  node,
  connectedEdges,
  onJumpTo,
}: {
  node: VisibleNode;
  connectedEdges?: SelectedEdge[];
  onJumpTo: (peer: PortPeer) => void;
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
        <Row label="Elapsed" value={formatElapsedNs(node.stats.elapsedNs)} />
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
          value={formatElapsedNs(node.transitive.elapsedNs)}
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
    {node.peers.length > 0 && (
      <Box mt={2}>
        <Text fontSize="xs" fontWeight="600" mb={1}>
          Connects outside this view
        </Text>
        {node.peers.map((p) => (
          <PeerRow key={p.address.join(".")} peer={p} onJumpTo={onJumpTo} />
        ))}
      </Box>
    )}
  </>
);

export const NodeDetailPanel = ({
  selection,
  onClose,
  onJumpTo,
}: {
  selection: Selection;
  onClose: () => void;
  onJumpTo: (peer: PortPeer) => void;
}) => {
  const title =
    selection.kind === "node"
      ? selection.node.label
      : selection.kind === "edge"
        ? `${selection.edge.sourceLabel} → ${selection.edge.targetLabel}`
        : `LIR ${selection.node.info.lirId}: ${selection.node.info.operator}`;
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
          onJumpTo={onJumpTo}
        />
      ) : selection.kind === "edge" ? (
        <EdgeRows edge={selection.edge} />
      ) : (
        <LirSummaryCard node={selection.node} />
      )}
    </Box>
  );
};
