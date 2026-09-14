// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Badge, Box, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import { Handle, type NodeProps, Position } from "@xyflow/react";
import React from "react";

import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";

import {
  type FlowGroupData,
  type FlowNodeData,
  HIGHLIGHT_COLORS,
  statLines,
  textColorFor,
} from "./nodeStyle";

// Selection (clicked) wins over the active search match when both apply to
// the same node; both are visually distinct from ordinary dimming.
const highlightShadow = (data: FlowNodeData): string | undefined => {
  if (data.selected) return `0 0 0 2px ${HIGHLIGHT_COLORS.selected}`;
  if (data.activeMatch) return `0 0 0 2px ${HIGHLIGHT_COLORS.activeMatch}`;
  return undefined;
};

// A subtle resting shadow (matching the app's other node-graph view) so
// cards read as raised surfaces even when unselected; highlightShadow
// overrides it with a colored ring for selected/matched nodes.
const RESTING_SHADOW = "0px 0.5px 2.5px 0 rgba(0, 0, 0, 0.08)";

const CardShell = ({
  data,
  filled = true,
  gutter,
  children,
}: {
  data: FlowNodeData;
  // A region is a collapsed subtree, not a single operator, so it reads as
  // an outlined container (border-only, like LirGroupNode's dashed box)
  // rather than a filled chip, to tell the two kinds apart at a glance.
  filled?: boolean;
  // Rendered flush against the card's left edge, outside the content
  // padding, so it can span the card's full height (see ExpandGutter).
  gutter?: React.ReactNode;
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      data-testid="node-shell"
      borderWidth={filled ? "1px" : "2px"}
      borderColor={filled ? undefined : data.color}
      borderRadius="8px"
      width="100%"
      height="100%"
      overflow="hidden"
      display="flex"
      background={filled ? data.color : colors.background.secondary}
      color={filled ? textColorFor(data.color) : colors.foreground.primary}
      opacity={data.dimmed ? 0.25 : 1}
      boxShadow={highlightShadow(data) ?? RESTING_SHADOW}
      // Every card is at least selectable, so every card says so. The
      // controls inside carry the distinctions.
      cursor="pointer"
    >
      {gutter}
      <Box px={2} py={1} flex="1" minWidth={0}>
        {children}
      </Box>
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </Box>
  );
};

// Expanding is the region's most common action and the one whose target was
// hardest to hit as a bare glyph, so it gets the card's whole left edge
// rather than a character's worth of space. The chevron doubles as the
// expanded/collapsed state indicator.
const ExpandGutter = ({ data }: { data: FlowNodeData }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      as="button"
      type="button"
      data-testid="region-toggle"
      aria-label={data.expanded ? "Collapse region" : "Expand region"}
      aria-expanded={data.expanded}
      flexShrink={0}
      width="24px"
      alignSelf="stretch"
      display="flex"
      alignItems="center"
      justifyContent="center"
      borderRightWidth="1px"
      borderColor={colors.border.secondary}
      cursor="pointer"
      _hover={{ background: colors.background.tertiary }}
      onClick={(e) => {
        e.stopPropagation();
        data.onToggleExpand?.(data.node.id);
      }}
    >
      {data.expanded ? "▾" : "▸"}
    </Box>
  );
};

// A link goes somewhere else, matching a file tree: the chevron opens the
// folder in place, the name opens the folder. Rendered as a button rather
// than an anchor because there is no URL to follow and because an anchor is
// natively draggable, which would fight dragging the canvas to pan.
const NavigateLink = ({
  label,
  testId,
  fontSize,
  onClick,
}: {
  label: string;
  testId: string;
  fontSize?: string;
  // Omitted when the destination is ambiguous: the click then falls through
  // to the node itself, selecting it so the detail panel can offer a choice.
  onClick?: () => void;
}) => (
  <TextLink
    as="button"
    type="button"
    data-testid={testId}
    textAlign="left"
    textStyle={fontSize ? undefined : "text-ui-med"}
    fontSize={fontSize}
    minWidth={0}
    onClick={
      onClick &&
      ((e: React.MouseEvent) => {
        e.stopPropagation();
        onClick();
      })
    }
  >
    {label}
  </TextLink>
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
        <Text textStyle="text-ui-med" noOfLines={2}>
          {data.node.label}
        </Text>
        {statLines(data.node).map((line) => (
          <Text key={line} textStyle="text-small" noOfLines={1}>
            {line}
          </Text>
        ))}
      </Box>
    </Tooltip>
  </CardShell>
);

// A region is a collapsed subtree by default. The chevron gutter expands it
// in place (children rendered nested inside); the label navigates into it,
// making it the view root. When expanded it becomes an outlined container
// with a transparent body so inner nodes and edges receive clicks, the same
// click-through contract LirGroupNode documents, which is also why the
// expanded header cannot use a full-height gutter.
export const RegionNode = ({ data }: NodeProps & { data: FlowNodeData }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const label = (
    <NavigateLink
      label={data.node.label}
      testId="region-enter"
      onClick={() => data.onNavigate?.(data.node.id)}
    />
  );
  if (data.expanded) {
    return (
      <Box width="100%" height="100%" position="relative" pointerEvents="none">
        <Box
          position="absolute"
          inset={0}
          borderWidth="2px"
          borderRadius="8px"
          borderColor={data.color}
        />
        <HStack
          position="absolute"
          top={0}
          left={0}
          right={0}
          px={1}
          py={1}
          pointerEvents="auto"
          spacing={1}
        >
          <Box
            as="button"
            type="button"
            data-testid="region-toggle"
            aria-label="Collapse region"
            aria-expanded
            flexShrink={0}
            px={2}
            py={1}
            lineHeight="1"
            borderRadius="4px"
            cursor="pointer"
            _hover={{ background: colors.background.tertiary }}
            onClick={(e) => {
              e.stopPropagation();
              data.onToggleExpand?.(data.node.id);
            }}
          >
            ▾
          </Box>
          {label}
        </HStack>
      </Box>
    );
  }
  return (
    <CardShell data={data} filled={false} gutter={<ExpandGutter data={data} />}>
      <HStack
        justifyContent="space-between"
        alignItems="flex-start"
        spacing={1}
      >
        {label}
        <Badge fontSize="2xs" flexShrink={0}>
          {data.node.childCount}
        </Badge>
      </HStack>
      {statLines(data.node).map((line) => (
        <Text key={line} textStyle="text-small" noOfLines={1}>
          {line}
        </Text>
      ))}
    </CardShell>
  );
};

// A port's peers live outside the current view, so the only thing to do with
// one is go there. Whether that is possible is encoded in how the label
// renders: plain text when the port leads nowhere, a link when it does, and
// a link with a count when the destination is ambiguous and the detail panel
// has to offer the choice.
export const PortNode = ({ data }: NodeProps & { data: FlowNodeData }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { peers } = data.node;
  const solePeer = peers.length === 1 ? peers[0] : null;
  return (
    <Box
      borderWidth="1px"
      borderRadius="full"
      px={2}
      display="flex"
      alignItems="center"
      background={colors.background.tertiary}
      opacity={data.dimmed ? 0.25 : 1}
      boxShadow={highlightShadow(data)}
      cursor="pointer"
    >
      {peers.length === 0 ? (
        <Text fontSize="2xs">{data.node.label}</Text>
      ) : (
        <NavigateLink
          label={
            peers.length > 1
              ? `${data.node.label} → ${peers.length}`
              : `${data.node.label} →`
          }
          testId="port-jump"
          fontSize="2xs"
          onClick={solePeer ? () => data.onJumpToPeer?.(solePeer) : undefined}
        />
      )}
      <Handle type="target" position={Position.Top} />
      <Handle type="source" position={Position.Bottom} />
    </Box>
  );
};

// A label-only wrapper around its members. Header is clickable
// (pointerEvents="auto"), body is not (pointerEvents="none"). For clicks on
// the body to pass through to underlying members, the node object this
// component is instantiated with must also set style={{pointerEvents:"none"}},
// since React Flow's own node wrapper defaults to pointer-events:all and
// this component's root cannot override an ancestor. Its width/height come
// from elk's auto-sized bounds, same as every other node in this file.
export const LirGroupNode = ({ data }: NodeProps & { data: FlowGroupData }) => (
  <Box width="100%" height="100%" position="relative" pointerEvents="none">
    <Box
      position="absolute"
      inset={0}
      borderWidth="2px"
      borderStyle="dashed"
      borderRadius="md"
      borderColor={data.color}
    />
    <Box
      position="absolute"
      top={0}
      left={0}
      px={2}
      py="1px"
      pointerEvents="auto"
      cursor="pointer"
      background={data.color}
      borderBottomRightRadius="md"
    >
      <Text fontSize="2xs" color={textColorFor(data.color)} noOfLines={1}>
        {data.label}
      </Text>
    </Box>
  </Box>
);
