// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useColorModeValue, useTheme } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { MaterializeTheme } from "~/theme";
import { formatIntervalShort } from "~/utils/format";

import { NodeKind, RenderableNode } from "./criticalPathRenderable";
import { useJourneyLinkState } from "./useJourneyLinkState";

const useNodeColors = (node: RenderableNode) => {
  const { colors } = useTheme<MaterializeTheme>();
  // Soft mint / dark forest — matches the design's "healthy" tint, less
  // saturated than `background.success` would feel here.
  const healthyBg = useColorModeValue("#F0FDF4", "#1F352A");
  // Soft cream-yellow / dark amber — less harsh than `background.warn` and
  // closer to the design's upstream-blocked tint.
  const upstreamBg = useColorModeValue("#FFFCF0", "#352F1A");

  const borderByKind: Record<NodeKind, string> = {
    primary: colors.accent.red,
    healthy: colors.accent.green,
    upstream: colors.accent.orange,
    offPath: colors.border.secondary,
  };
  const bgByKind: Record<NodeKind, string> = {
    primary: colors.background.error,
    healthy: healthyBg,
    upstream: upstreamBg,
    offPath: colors.background.primary,
  };

  // The probe itself uses purple-dashed border — except when it stands out as
  // the primary bottleneck (red) or its lag is healthy (green).
  const probeOverridesKind = node.kind === "upstream";
  const borderColor =
    node.isProbe && probeOverridesKind
      ? colors.accent.brightPurple
      : borderByKind[node.kind];
  const bg = node.isProbe ? colors.background.primary : bgByKind[node.kind];

  return { borderColor, bg };
};

export interface CriticalPathGraphNodeProps {
  node: RenderableNode;
  /** Id of the object the page is currently rendering. Used to extend the
   *  breadcrumb journey when navigating to a different node. */
  pageObjectId: string;
  /** True when the user has clicked the off-path counter to splice in
   *  this bottleneck's siblings. */
  expanded: boolean;
  /** True when the user has clicked the node body to read the full name —
   *  lifts to natural height with a raised z-index. */
  selected: boolean;
  left: number;
  top: number;
  width: number;
  height: number;
  onToggleExpand: () => void;
  onSelect: () => void;
}

export const CriticalPathGraphNode = ({
  node,
  pageObjectId,
  expanded,
  selected,
  left,
  top,
  width,
  height,
  onToggleExpand,
  onSelect,
}: CriticalPathGraphNodeProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const { borderColor, bg } = useNodeColors(node);
  const lagText = node.lag ? formatIntervalShort(node.lag) : "—";
  const borderStyle = node.isProbe || expanded ? "dashed" : "solid";
  const isOffPath = node.kind === "offPath";

  const journeyLinkState = useJourneyLinkState(pageObjectId);
  const objectLinkTarget = `../${node.parentSourceId ?? node.id}`;

  return (
    <Box
      position="absolute"
      left={`${left}px`}
      top={`${top}px`}
      width={`${width}px`}
      minHeight={`${height}px`}
      height={selected ? "auto" : `${height}px`}
      zIndex={selected ? 10 : 1}
      boxShadow={selected ? shadows.input.focus : undefined}
      borderRadius="md"
      borderWidth="2px"
      borderStyle={borderStyle}
      borderColor={borderColor}
      bg={bg}
      opacity={isOffPath ? 0.65 : 1}
      px={3}
      py={1}
      display="flex"
      flexDirection="column"
      justifyContent="center"
      overflow={selected ? "visible" : "hidden"}
      cursor="pointer"
      transition="box-shadow 0.15s, transform 0.15s"
      _hover={{
        boxShadow: selected ? shadows.input.focus : shadows.level1,
      }}
      onClick={onSelect}
    >
      {node.isProbe ? (
        <Text
          textStyle="text-ui-med"
          noOfLines={selected ? undefined : 1}
          wordBreak={selected ? "break-all" : undefined}
        >
          {node.name}
        </Text>
      ) : (
        <Text
          as={RouterLink}
          to={objectLinkTarget}
          relative="path"
          state={journeyLinkState}
          onClick={(e) => e.stopPropagation()}
          textStyle="text-ui-med"
          noOfLines={selected ? undefined : 1}
          wordBreak={selected ? "break-all" : undefined}
          _hover={{
            color: colors.accent.brightPurple,
            textDecoration: "underline",
          }}
        >
          {node.name}
        </Text>
      )}
      <Text
        textStyle="text-small"
        color={colors.foreground.secondary}
        noOfLines={selected ? undefined : 1}
      >
        {lagText}
        {node.objectType === "source" && (
          <>
            {" · "}
            <Box
              as="span"
              bg={colors.background.secondary}
              color={colors.foreground.secondary}
              px="1"
              py="0.5"
              borderRadius="sm"
              fontSize="xs"
            >
              source
            </Box>
          </>
        )}
        {node.offPathCount > 0 && (
          <>
            {" · "}
            <Box
              as="span"
              cursor="pointer"
              textDecoration="underline"
              onClick={(e: React.MouseEvent) => {
                e.stopPropagation();
                onToggleExpand();
              }}
            >
              {node.offPathCount} off-path
            </Box>
          </>
        )}
      </Text>
    </Box>
  );
};
