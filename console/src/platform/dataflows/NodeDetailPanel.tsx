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
import { formatSkew, prettyPrintChannelType } from "./nodeStyle";

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

// onJumpTo is omitted for the compact form used inside a node's "Connected
// edges" list (which can be long for a hub node): landing lists there would
// swamp the panel. The dedicated edge-selection view always passes it, to
// show where a merged edge's real channels land inside a collapsed region.
const EdgeRows = ({
  edge,
  onJumpTo,
}: {
  edge: SelectedEdge;
  onJumpTo?: (peer: PortPeer) => void;
}) => (
  <>
    <Row label="Records" value={edge.messagesSent.toString()} />
    <Row label="Batches" value={edge.batchesSent.toString()} />
    <TypeRow channelTypes={edge.channelTypes} />
    {onJumpTo && edge.sourceLandings.length > 0 && (
      <Box mt={2}>
        <Text fontSize="xs" fontWeight="600" mb={1}>
          Inside {edge.sourceLabel}
        </Text>
        {edge.sourceLandings.map((p) => (
          <PeerRow key={p.address.join(".")} peer={p} onJumpTo={onJumpTo} />
        ))}
      </Box>
    )}
    {onJumpTo && edge.targetLandings.length > 0 && (
      <Box mt={2}>
        <Text fontSize="xs" fontWeight="600" mb={1}>
          Inside {edge.targetLabel}
        </Text>
        {edge.targetLandings.map((p) => (
          <PeerRow key={p.address.join(".")} peer={p} onJumpTo={onJumpTo} />
        ))}
      </Box>
    )}
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

// Same Row-based shape as NodeDetail, rather than a visually distinct card,
// so a LIR group's details read like every other selection in this panel.
const LirGroupDetail = ({ node }: { node: LirTreeNode }) => (
  <>
    <Row label="Export" value={node.info.exportId} />
    <Row label="LIR ID" value={node.info.lirId} />
    <Row label="Members" value={String(node.memberIds.length)} />
    <Row label="Records" value={node.summary.arrangementRecords.toString()} />
    <Row
      label="Memory"
      value={formatBytesShort(node.summary.arrangementSize)}
    />
    <Row label="Elapsed" value={formatElapsedNs(node.summary.elapsedNs)} />
  </>
);

const NodeDetail = ({
  node,
  connectedEdges,
  onJumpTo,
  onSelectLir,
}: {
  node: VisibleNode;
  connectedEdges?: SelectedEdge[];
  onJumpTo: (peer: PortPeer) => void;
  onSelectLir: (exportId: string, lirId: string) => void;
}) => (
  <>
    <Row label="Kind" value={node.kind} />
    {node.address && <Row label="Address" value={node.address.join(".")} />}
    {node.operatorId !== null && (
      <Row label="Operator ID" value={node.operatorId.toString()} />
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
    {node.transitiveSkew && (
      <>
        <Row label="CPU skew" value={formatSkew(node.transitiveSkew.cpuSkew)} />
        <Row
          label="Memory skew"
          value={formatSkew(node.transitiveSkew.memorySkew)}
        />
      </>
    )}
    {node.lir.length > 0 && (
      <Box mt={2}>
        <Text fontSize="xs" fontWeight="600" mb={1}>
          LIR
        </Text>
        {node.lir.map((l) => (
          <Text
            key={`${l.exportId}/${l.lirId}`}
            fontSize="xs"
            color="blue.600"
            textDecoration="underline"
            cursor="pointer"
            onClick={() => onSelectLir(l.exportId, l.lirId)}
          >
            LIR {l.lirId} ({l.exportId}): {l.operator}
          </Text>
        ))}
      </Box>
    )}
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
  collapsed,
  onToggleCollapsed,
  onClose,
  onJumpTo,
  onSelectLir,
}: {
  selection: Selection;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onClose: () => void;
  onJumpTo: (peer: PortPeer) => void;
  onSelectLir: (exportId: string, lirId: string) => void;
}) => {
  if (collapsed) {
    return (
      <Box
        width="24px"
        borderLeftWidth="1px"
        flexShrink={0}
        display="flex"
        justifyContent="center"
        pt={2}
      >
        <Button
          size="2xs"
          variant="ghost"
          onClick={onToggleCollapsed}
          aria-label="Expand details panel"
        >
          «
        </Button>
      </Box>
    );
  }
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
        <HStack spacing={1}>
          <Button
            size="2xs"
            variant="ghost"
            onClick={onToggleCollapsed}
            aria-label="Collapse details panel"
          >
            »
          </Button>
          <CloseButton onClick={onClose} />
        </HStack>
      </HStack>
      {selection.kind === "node" ? (
        <NodeDetail
          node={selection.node}
          connectedEdges={selection.connectedEdges}
          onJumpTo={onJumpTo}
          onSelectLir={onSelectLir}
        />
      ) : selection.kind === "edge" ? (
        <EdgeRows edge={selection.edge} onJumpTo={onJumpTo} />
      ) : (
        <LirGroupDetail node={selection.node} />
      )}
    </Box>
  );
};
