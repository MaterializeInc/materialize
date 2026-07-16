// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, HStack, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import {
  SidebarItem,
  SidebarItemLabel,
  SidebarItemValue,
  SidebarSection,
  type SidebarSectionProps,
} from "~/components/WorkflowGraph/Sidebar";
import { ArrowRightIcon, ChevronLeftIcon, ChevronRightIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { formatBytesShort, formatElapsedNs } from "~/utils/format";

import {
  type LirTreeNode,
  type PortPeer,
  type VisibleNode,
} from "./dataflowGraph";
import type { SelectedEdge } from "./DataflowGraphView";
import { formatCount, formatSkew, prettyPrintChannelType } from "./nodeStyle";

export type Selection =
  | { kind: "node"; node: VisibleNode; connectedEdges?: SelectedEdge[] }
  | { kind: "edge"; edge: SelectedEdge }
  | { kind: "lirGroup"; node: LirTreeNode };

interface RowProps {
  label: string;
  value: React.ReactNode;
}

const Row = ({ label, value }: RowProps) => (
  <SidebarItem px={0} py="1">
    <SidebarItemLabel>{label}</SidebarItemLabel>
    <SidebarItemValue fontSize="xs" textStyle="monospace">
      {value}
    </SidebarItemValue>
  </SidebarItem>
);

// SidebarSection's header defaults to the workflow sidebar's own px="4",
// which double-counts here: this panel already insets its whole Box, and
// every row in it (via Row's own px={0} override) sits flush with that,
// so the header needs the same override to stay aligned with them.
const Section = (props: SidebarSectionProps) => (
  <SidebarSection {...props} headerProps={{ px: 0, ...props.headerProps }} />
);

// Intl.NumberFormat's grouping separators would get copied along with the
// digits if this were plain text ("221,245,721" pastes with the commas
// still in it): user-select:none on each separator excludes it from the
// clipboard while leaving it fully visible.
const GroupedCount = ({ n }: { n: bigint }) => {
  const groups = formatCount(n).split(",");
  return (
    <>
      {groups.map((group, i) => (
        <React.Fragment key={i}>
          {i > 0 && (
            <Text as="span" userSelect="none">
              ,
            </Text>
          )}
          {group}
        </React.Fragment>
      ))}
    </>
  );
};

interface TypeRowProps {
  channelTypes: string[];
}

// Channel types are Rust container type signatures (pretty-printed, but
// still nested-generic shaped, e.g. "[Rc<OrdValBatch<...>>]"): long enough
// that they get their own full-width stacked row instead of Row's
// space-between line. wordBreak wraps rather than clipping, so a longer
// type still reads in full instead of overflowing the panel.
const TypeRow = ({ channelTypes }: TypeRowProps) => (
  <Box>
    <SidebarItemLabel>Type</SidebarItemLabel>
    <Text fontSize="xs" textStyle="monospace" wordBreak="break-all">
      {channelTypes.map(prettyPrintChannelType).join(", ") || "unknown"}
    </Text>
  </Box>
);

interface EdgeRowsProps {
  edge: SelectedEdge;
  onJumpTo: (peer: PortPeer) => void;
}

// The dedicated edge-selection view (EdgeDetail), showing where a merged
// edge's real channels land inside a collapsed region.
const EdgeRows = ({ edge, onJumpTo }: EdgeRowsProps) => (
  <>
    <Row label="Records" value={<GroupedCount n={edge.messagesSent} />} />
    <Row label="Batches" value={<GroupedCount n={edge.batchesSent} />} />
    <TypeRow channelTypes={edge.channelTypes} />
    {edge.sourceLandings.length > 0 && (
      <Section title={`Inside ${edge.sourceLabel}`}>
        {edge.sourceLandings.map((p) => (
          <PeerRow key={p.address.join(".")} peer={p} onJumpTo={onJumpTo} />
        ))}
      </Section>
    )}
    {edge.targetLandings.length > 0 && (
      <Section title={`Inside ${edge.targetLabel}`}>
        {edge.targetLandings.map((p) => (
          <PeerRow key={p.address.join(".")} peer={p} onJumpTo={onJumpTo} />
        ))}
      </Section>
    )}
  </>
);

interface EdgeDetailProps {
  edge: SelectedEdge;
  onJumpTo: (peer: PortPeer) => void;
  onSelectNode: (id: string) => void;
}

// Source/target are always in-view for a drawn edge (unlike a port's
// out-of-view peers), so selecting them is a plain re-selection, not a
// scope-crossing jump.
const EdgeDetail = ({ edge, onJumpTo, onSelectNode }: EdgeDetailProps) => (
  <>
    <Row label="Kind" value="edge" />
    <SidebarItem px={0} py="1">
      <SidebarItemLabel>Source</SidebarItemLabel>
      <TextLink
        as="button"
        fontSize="xs"
        onClick={() => onSelectNode(edge.source)}
      >
        {edge.sourceLabel}
      </TextLink>
    </SidebarItem>
    <SidebarItem px={0} py="1">
      <SidebarItemLabel>Target</SidebarItemLabel>
      <TextLink
        as="button"
        fontSize="xs"
        onClick={() => onSelectNode(edge.target)}
      >
        {edge.targetLabel}
      </TextLink>
    </SidebarItem>
    <EdgeRows edge={edge} onJumpTo={onJumpTo} />
  </>
);

// A jump target lives outside the current view (or, for a port's own
// upstream/downstream, may just not be worth drawing an edge to), so its
// only affordance is a jump: navigate there instead of tracing a line to
// it. Styled as an inline link rather than a button, with ArrowRightIcon
// rather than ExternalLinkIcon, since this jumps within the same
// canvas/page, not out to a new one.
const JumpLink = ({
  label,
  onClick,
}: {
  label: string;
  onClick: () => void;
}) => (
  <TextLink
    as="button"
    display="flex"
    alignItems="center"
    minWidth={0}
    fontSize="xs"
    onClick={onClick}
  >
    <Text as="span" noOfLines={1}>
      {label}
    </Text>
    <ArrowRightIcon boxSize="3" ml={1} flexShrink={0} />
  </TextLink>
);

interface PeerRowProps {
  peer: PortPeer;
  onJumpTo: (peer: PortPeer) => void;
}

// Used for a merged edge's landings, where each landing is its own real
// channel and can genuinely have its own stats and type (see EdgeRowsProps).
const PeerRow = ({ peer, onJumpTo }: PeerRowProps) => (
  <Box mb={2}>
    <SidebarItem px={0} py="1">
      <SidebarItemLabel>Target</SidebarItemLabel>
      <JumpLink label={peer.label} onClick={() => onJumpTo(peer)} />
    </SidebarItem>
    <Row label="Records" value={<GroupedCount n={peer.messagesSent} />} />
    <Row label="Batches" value={<GroupedCount n={peer.batchesSent} />} />
    <TypeRow channelTypes={peer.channelTypes} />
  </Box>
);

interface LirGroupDetailProps {
  node: LirTreeNode;
}

// Same Row-based shape as NodeDetail, rather than a visually distinct card,
// so a LIR group's details read like every other selection in this panel.
const LirGroupDetail = ({ node }: LirGroupDetailProps) => (
  <>
    <Row label="Export" value={node.info.exportId} />
    <Row label="LIR ID" value={node.info.lirId} />
    <Row label="Members" value={String(node.memberIds.length)} />
    <Row
      label="Records"
      value={<GroupedCount n={node.summary.arrangementRecords} />}
    />
    <Row
      label="Memory"
      value={formatBytesShort(node.summary.arrangementSize)}
    />
    <Row label="Elapsed" value={formatElapsedNs(node.summary.elapsedNs)} />
    <Row
      label="Schedules"
      value={<GroupedCount n={node.summary.scheduleCount} />}
    />
  </>
);

interface PortTotals {
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
}

// A port doesn't transform data, so its in-view connected edge and its
// out-of-view peers are the same channel end to end: prefer summing
// whichever side has an in-view edge (the aggregate, authoritative crossing),
// falling back to summing peers only when there's no in-view edge at all
// (e.g. a view-root boundary port with nothing drawn for its own side).
const portTotals = (edges: SelectedEdge[], peers: PortPeer[]): PortTotals => {
  const source: {
    messagesSent: bigint;
    batchesSent: bigint;
    channelTypes: string[];
  }[] = edges.length > 0 ? edges : peers;
  return {
    messagesSent: source.reduce((sum, s) => sum + s.messagesSent, 0n),
    batchesSent: source.reduce((sum, s) => sum + s.batchesSent, 0n),
    channelTypes: [...new Set(source.flatMap((s) => s.channelTypes))],
  };
};

interface PortLinkItem {
  key: string;
  label: string;
  onJumpTo?: () => void;
}

const PortLinkList = ({ items }: { items: PortLinkItem[] }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (items.length === 0) {
    return (
      <Text fontSize="xs" color={colors.foreground.tertiary}>
        None
      </Text>
    );
  }
  return (
    <>
      {items.map((item) =>
        item.onJumpTo ? (
          <Box key={item.key} mb={1}>
            <JumpLink label={item.label} onClick={item.onJumpTo} />
          </Box>
        ) : (
          <Text key={item.key} fontSize="xs" noOfLines={1} mb={1}>
            {item.label}
          </Text>
        ),
      )}
    </>
  );
};

interface PortDetailProps {
  node: VisibleNode;
  connectedEdges?: SelectedEdge[];
  onJumpTo: (peer: PortPeer) => void;
}

// An "input" port's peers are upstream of it (out-of-view senders feeding
// in); an "output" port's peers are downstream (out-of-view receivers). The
// in-view connected edge is always the other side. See VisibleNode.direction.
const PortDetail = ({ node, connectedEdges, onJumpTo }: PortDetailProps) => {
  const edges = connectedEdges ?? [];
  const totals = portTotals(edges, node.peers);
  const peerItems: PortLinkItem[] = node.peers.map((p) => ({
    key: p.address.join("."),
    label: p.label,
    onJumpTo: () => onJumpTo(p),
  }));
  const edgeItems = (labelOf: (e: SelectedEdge) => string): PortLinkItem[] =>
    edges.map((e) => ({ key: e.id, label: labelOf(e) }));
  let upstream: PortLinkItem[];
  let downstream: PortLinkItem[];
  if (node.direction === "input") {
    upstream = peerItems;
    downstream = edgeItems((e) => e.targetLabel);
  } else if (node.direction === "output") {
    upstream = edgeItems((e) => e.sourceLabel);
    downstream = peerItems;
  } else {
    // Every real port sets direction at construction; this is unreachable
    // in practice, but falls back to one undifferentiated list rather than
    // guessing which side is upstream.
    upstream = [];
    downstream = [
      ...edgeItems((e) => `${e.sourceLabel} → ${e.targetLabel}`),
      ...peerItems,
    ];
  }
  return (
    <>
      <Row label="Kind" value={node.kind} />
      <TypeRow channelTypes={totals.channelTypes} />
      <Row label="Records" value={<GroupedCount n={totals.messagesSent} />} />
      <Row label="Batches" value={<GroupedCount n={totals.batchesSent} />} />
      <Section title="Upstream">
        <PortLinkList items={upstream} />
      </Section>
      <Section title="Downstream">
        <PortLinkList items={downstream} />
      </Section>
    </>
  );
};

interface NodeDetailProps {
  node: VisibleNode;
  onSelectLir: (exportId: string, lirId: string) => void;
  workerCount: number;
}

const NodeDetail = ({ node, onSelectLir, workerCount }: NodeDetailProps) => {
  // On a single-worker replica there's nothing to compare across workers:
  // every ratio is trivially 1 (or the "no data" sentinel), so the section
  // is hidden rather than shown with meaningless numbers. Same threshold as
  // the toolbar's skew heatmap toggle (DataflowToolbar.tsx).
  const showSkew = workerCount > 1;
  return (
    <>
      <Row label="Kind" value={node.kind} />
      {node.address && <Row label="Address" value={node.address.join(".")} />}
      {node.operatorId !== null && (
        <Row label="Operator ID" value={node.operatorId.toString()} />
      )}
      {node.childCount > 0 && (
        <Row label="Children" value={String(node.childCount)} />
      )}
      {node.own && (
        <Section title="Own">
          <Row
            label="Arranged records"
            value={<GroupedCount n={node.own.arrangementRecords} />}
          />
          <Row
            label="Arrangement size"
            value={formatBytesShort(node.own.arrangementSize)}
          />
          <Row label="Elapsed" value={formatElapsedNs(node.own.elapsedNs)} />
          <Row
            label="Schedules"
            value={<GroupedCount n={node.own.scheduleCount} />}
          />
        </Section>
      )}
      {node.childCount > 0 && (
        <Row label="Overhead" value={formatElapsedNs(node.overheadNs ?? 0n)} />
      )}
      {node.transitive && node.childCount > 0 && (
        <Section title="Subtree">
          <Row
            label="Subtree records"
            value={<GroupedCount n={node.transitive.arrangementRecords} />}
          />
          <Row
            label="Subtree size"
            value={formatBytesShort(node.transitive.arrangementSize)}
          />
          <Row
            label="Subtree elapsed"
            value={formatElapsedNs(node.transitive.elapsedNs)}
          />
          <Row
            label="Subtree schedules"
            value={<GroupedCount n={node.transitive.scheduleCount} />}
          />
        </Section>
      )}
      {showSkew && node.ownSkew && (
        <Section title="Skew">
          <Row label="CPU skew" value={formatSkew(node.ownSkew.cpuSkew)} />
          <Row
            label="Memory skew"
            value={formatSkew(node.ownSkew.memorySkew)}
          />
          <Row
            label="Schedule skew"
            value={formatSkew(node.ownSkew.scheduleSkew)}
          />
        </Section>
      )}
      {showSkew && node.transitiveSkew && node.childCount > 0 && (
        <Section title="Subtree skew">
          <Row
            label="Subtree CPU skew"
            value={formatSkew(node.transitiveSkew.cpuSkew)}
          />
          <Row
            label="Subtree memory skew"
            value={formatSkew(node.transitiveSkew.memorySkew)}
          />
          <Row
            label="Subtree schedule skew"
            value={formatSkew(node.transitiveSkew.scheduleSkew)}
          />
        </Section>
      )}
      {node.lir.length > 0 && (
        <Section title="LIR">
          {node.lir.map((l) => (
            <TextLink
              key={`${l.exportId}/${l.lirId}`}
              as="button"
              display="block"
              fontSize="xs"
              textAlign="left"
              onClick={() => onSelectLir(l.exportId, l.lirId)}
            >
              LIR {l.lirId} ({l.exportId}): {l.operator}
            </TextLink>
          ))}
        </Section>
      )}
    </>
  );
};

export interface NodeDetailPanelProps {
  selection: Selection;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onJumpTo: (peer: PortPeer) => void;
  onSelectLir: (exportId: string, lirId: string) => void;
  onSelectNode: (id: string) => void;
  workerCount: number;
}

export const NodeDetailPanel = ({
  selection,
  collapsed,
  onToggleCollapsed,
  onJumpTo,
  onSelectLir,
  onSelectNode,
  workerCount,
}: NodeDetailPanelProps) => {
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
          <ChevronLeftIcon boxSize="3" />
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
        <Button
          size="2xs"
          variant="ghost"
          onClick={onToggleCollapsed}
          aria-label="Collapse details panel"
        >
          <ChevronRightIcon boxSize="3" />
        </Button>
      </HStack>
      {selection.kind === "node" ? (
        selection.node.kind === "port" ? (
          <PortDetail
            node={selection.node}
            connectedEdges={selection.connectedEdges}
            onJumpTo={onJumpTo}
          />
        ) : (
          <NodeDetail
            node={selection.node}
            onSelectLir={onSelectLir}
            workerCount={workerCount}
          />
        )
      ) : selection.kind === "edge" ? (
        <EdgeDetail
          edge={selection.edge}
          onJumpTo={onJumpTo}
          onSelectNode={onSelectNode}
        />
      ) : (
        <LirGroupDetail node={selection.node} />
      )}
    </Box>
  );
};
