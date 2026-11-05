// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, useTheme } from "@chakra-ui/react";
import React, { useLayoutEffect, useRef, useState } from "react";

import { MaterializeTheme } from "~/theme";

export interface CellFrameProps {
  /** Controls beside the editor's first line, such as run. */
  gutter: React.ReactNode;
  editor: React.ReactNode;
  /** Beside the editor's first line, at the cell's right edge. */
  actions?: React.ReactNode;
  /** Shown under the editor, indented to where its code starts. */
  below?: React.ReactNode;
  barColor?: string;
  /** Widens the bar, as focus does. */
  selected?: boolean;
}

/**
 * A cell's layout: a bar on the left that marks the cell and widens while
 * it is selected or has focus, a gutter, the editor, and what belongs under
 * it.
 */
export const CellFrame = ({
  gutter,
  editor,
  actions,
  below,
  barColor,
  selected = false,
}: CellFrameProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const rowRef = useRef<HTMLDivElement>(null);
  const editorRef = useRef<HTMLDivElement>(null);
  const [inset, setInset] = useState(0);

  // The code starts after the gutter and the editor's line numbers, whose
  // width grows with the line count, so it is measured rather than assumed.
  useLayoutEffect(() => {
    const row = rowRef.current;
    const editorBox = editorRef.current;
    if (!row || !editorBox) return;
    const measure = () => {
      const content = editorBox.querySelector(".cm-content");
      if (!content) return;
      setInset(
        Math.round(
          content.getBoundingClientRect().left -
            row.getBoundingClientRect().left,
        ),
      );
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(row);
    const gutters = editorBox.querySelector(".cm-gutters");
    if (gutters) observer.observe(gutters);
    return () => observer.disconnect();
  }, []);

  return (
    <Flex role="group" minW="0">
      <Box
        width={selected ? "5px" : "3px"}
        flexShrink={0}
        borderRadius="full"
        bg={barColor ?? colors.border.primary}
        transition="background-color 0.15s, width 0.15s"
        _groupFocusWithin={{ width: "5px" }}
      />
      <Box
        flex="1"
        minW="0"
        pl={selected ? "0.5" : "1"}
        _groupFocusWithin={{ pl: "0.5" }}
      >
        <Flex ref={rowRef} alignItems="flex-start">
          <HStack spacing="0" flexShrink={0}>
            {gutter}
          </HStack>
          <Box ref={editorRef} flex="1" minW="0">
            {editor}
          </Box>
          {actions && (
            <HStack spacing="0" flexShrink={0} pl="2" pr="1">
              {actions}
            </HStack>
          )}
        </Flex>
        {below && <Box pl={`${inset}px`}>{below}</Box>}
      </Box>
    </Flex>
  );
};
