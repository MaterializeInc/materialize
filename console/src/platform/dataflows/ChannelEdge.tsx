// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  type EdgeProps,
  getBezierPath,
} from "@xyflow/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import type { PortPeer } from "./dataflowGraph";
import { HIGHLIGHT_COLORS, prettyPrintChannelType } from "./nodeStyle";

export type ChannelEdgeData = {
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
  dimmed: boolean;
  selected: boolean;
  // Touches the selected node without being the clicked thing itself (an
  // edge whose own id is selected gets `selected` instead), so it reads as
  // a lighter, secondary emphasis.
  connected: boolean;
  // Where this edge's real channel(s) land inside a collapsed source/target
  // region, mirroring VisibleEdge's fields of the same name. Only ever
  // shown as an inline canvas button when there's exactly one: with several
  // (a merge fanned out to multiple real landings) a single click can't say
  // which one, so the picker stays in the detail panel's landing list.
  sourceLandings: PortPeer[];
  targetLandings: PortPeer[];
  onJumpTo?: (peer: PortPeer) => void;
};

const compactCount = Intl.NumberFormat("default", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const compact = (n: bigint) => compactCount.format(n);

export const ChannelEdge = (props: EdgeProps & { data: ChannelEdgeData }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [path, labelX, labelY] = getBezierPath(props);
  const {
    messagesSent,
    batchesSent,
    channelTypes,
    dimmed,
    selected,
    connected,
    sourceLandings,
    targetLandings,
    onJumpTo,
  } = props.data;
  const idle = messagesSent === 0n;
  // Inline label stays terse: compact record/batch counts only. Exact figures
  // and the full container type names live in the tooltip, since those Rust
  // type strings are long enough to swamp the canvas. Selecting an idle edge
  // still shows its (zero) counts, confirming the click landed.
  const label =
    idle && !selected
      ? ""
      : `${compact(messagesSent)} / ${compact(batchesSent)}`;
  const prettyTypes = channelTypes.map(prettyPrintChannelType);
  const tooltip = idle
    ? prettyTypes.join(", ") || "unknown channel type"
    : `${messagesSent} records / ${batchesSent} batches` +
      (prettyTypes.length > 0 ? ` · ${prettyTypes.join(", ")}` : "");
  // Only an unambiguous (single-landing) side gets a one-click button here;
  // see the sourceLandings/targetLandings doc comment for why a fan-out
  // doesn't.
  const sourceLanding =
    selected && sourceLandings.length === 1 ? sourceLandings[0] : null;
  const targetLanding =
    selected && targetLandings.length === 1 ? targetLandings[0] : null;
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          strokeDasharray: idle ? "6 4" : undefined,
          opacity: dimmed ? 0.15 : 1,
          stroke: selected
            ? HIGHLIGHT_COLORS.selected
            : connected
              ? HIGHLIGHT_COLORS.connected
              : undefined,
          strokeWidth: selected ? 2.5 : connected ? 1.75 : undefined,
        }}
      />
      {label && (
        <EdgeLabelRenderer>
          <Tooltip label={tooltip}>
            <Text
              position="absolute"
              transform={`translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`}
              fontSize="2xs"
              background={colors.background.secondary}
              px={1}
              borderRadius="sm"
              opacity={dimmed ? 0.15 : 1}
              className="nopan"
            >
              {label}
            </Text>
          </Tooltip>
        </EdgeLabelRenderer>
      )}
      {onJumpTo && (sourceLanding || targetLanding) && (
        <EdgeLabelRenderer>
          <HStack
            position="absolute"
            transform={`translate(-50%, 0) translate(${labelX}px, ${labelY + 10}px)`}
            className="nodrag nopan"
            // EdgeLabelRenderer's container has `pointer-events: none` by
            // default (so it never blocks panning/clicking the canvas
            // beneath it); nodrag/nopan only stop React Flow's own
            // drag/pan gestures from starting here, they don't restore
            // pointer-events, so without this the buttons render but never
            // receive clicks.
            pointerEvents="all"
            spacing={1}
          >
            {sourceLanding && (
              <Tooltip label={`Jump to ${sourceLanding.label}`}>
                <Button
                  size="2xs"
                  variant="outline"
                  background={colors.background.primary}
                  // EdgeLabelRenderer portals into a different DOM subtree,
                  // but React bubbles synthetic events along the component
                  // tree, not the DOM tree: without this, the click still
                  // reaches ReactFlow's onEdgeClick and the edge "steals"
                  // the click back (reselecting the edge instead of
                  // jumping).
                  onClick={(e) => {
                    e.stopPropagation();
                    onJumpTo(sourceLanding);
                  }}
                >
                  ← {sourceLanding.label}
                </Button>
              </Tooltip>
            )}
            {targetLanding && (
              <Tooltip label={`Jump to ${targetLanding.label}`}>
                <Button
                  size="2xs"
                  variant="outline"
                  background={colors.background.primary}
                  onClick={(e) => {
                    e.stopPropagation();
                    onJumpTo(targetLanding);
                  }}
                >
                  {targetLanding.label} →
                </Button>
              </Tooltip>
            )}
          </HStack>
        </EdgeLabelRenderer>
      )}
    </>
  );
};
