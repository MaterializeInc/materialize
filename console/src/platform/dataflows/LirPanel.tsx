// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import { ChevronLeftIcon, ChevronRightIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { CollapsedPanelStrip } from "./CollapsedPanelStrip";
import {
  type DataflowStructure,
  lirIndex,
  lirTree,
  type LirTreeNode,
} from "./dataflowGraph";

interface LirRowProps {
  node: LirTreeNode;
  structure: DataflowStructure;
  selectedKey?: string;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (exportId: string, lirId: string) => void;
}

const LirRow = ({
  node,
  structure,
  selectedKey,
  onHighlight,
  onSelect,
}: LirRowProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const label = `LIR ${node.info.lirId}: ${node.info.operator}`;
  const selected = node.key === selectedKey;
  // The dataflow root is never drawn on the canvas, so a span covering only
  // it has nothing to show either. Spans that only refer to an input or a
  // let binding (Read, Stream, Arranged) build no operators of their own.
  const hasOperators = node.memberIds.some(
    (id) => (structure.nodes.get(id)?.address.length ?? 0) > 1,
  );
  const rowProps = {
    display: "block",
    width: "100%",
    textAlign: "left",
    fontSize: "xs",
    pl: node.info.nesting * 3,
    py: "1px",
  } as const;
  const highlight = () => onHighlight(new Set(node.memberIds));
  const clearHighlight = () => onHighlight(null);
  return (
    <>
      {hasOperators ? (
        // A link, since a click moves the canvas to the span's operators.
        // Focus mirrors hover for keyboard users.
        <TextLink
          as="button"
          type="button"
          {...rowProps}
          aria-current={selected || undefined}
          fontWeight={selected ? "600" : undefined}
          background={selected ? colors.background.secondary : undefined}
          _focusVisible={{ background: colors.background.secondary }}
          onMouseEnter={highlight}
          onMouseLeave={clearHighlight}
          onFocus={highlight}
          onBlur={clearHighlight}
          onClick={() => onSelect(node.info.exportId, node.info.lirId)}
        >
          {label}
        </TextLink>
      ) : (
        <Tooltip
          label="No operators of its own in this dataflow"
          placement="right"
          openDelay={300}
        >
          <Text {...rowProps} color={colors.foreground.secondary}>
            {label}
          </Text>
        </Tooltip>
      )}
      {node.children.map((child) => (
        <LirRow
          key={child.key}
          node={child}
          structure={structure}
          selectedKey={selectedKey}
          onHighlight={onHighlight}
          onSelect={onSelect}
        />
      ))}
    </>
  );
};

export interface LirPanelProps {
  structure: DataflowStructure;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onHighlight: (ids: ReadonlySet<string> | null) => void;
  onSelect: (exportId: string, lirId: string) => void;
  /** The `${exportId}/${lirId}` key of the selected span, marked in the list. */
  selectedKey?: string;
}

export const LirPanel = ({
  structure,
  collapsed,
  onToggleCollapsed,
  onHighlight,
  onSelect,
  selectedKey,
}: LirPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const index = React.useMemo(() => lirIndex(structure), [structure]);
  const tree = React.useMemo(
    () => lirTree(structure, index),
    [structure, index],
  );

  if (index.size === 0) return null;
  if (collapsed) {
    return (
      <CollapsedPanelStrip
        label="LIR"
        ariaLabel="Expand LIR panel"
        chevron={<ChevronRightIcon boxSize="3" />}
        onClick={onToggleCollapsed}
        borderRightWidth="1px"
      />
    );
  }
  return (
    <Box
      width="280px"
      borderRightWidth="1px"
      p={3}
      overflowY="auto"
      flexShrink={0}
    >
      <HStack justifyContent="space-between" mb={2}>
        <Text fontSize="xs" fontWeight="600">
          LIR
        </Text>
        <Button
          size="2xs"
          variant="ghost"
          onClick={onToggleCollapsed}
          aria-label="Collapse LIR panel"
        >
          <ChevronLeftIcon boxSize="3" />
        </Button>
      </HStack>
      {[...tree.entries()].map(([exportId, roots]) => (
        <Box key={exportId} mb={3}>
          <Text fontSize="xs" color={colors.foreground.secondary}>
            {exportId}
          </Text>
          {roots.map((root) => (
            <LirRow
              key={root.key}
              node={root}
              structure={structure}
              selectedKey={selectedKey}
              onHighlight={onHighlight}
              onSelect={onSelect}
            />
          ))}
        </Box>
      ))}
    </Box>
  );
};
