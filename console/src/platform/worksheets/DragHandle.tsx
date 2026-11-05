// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createIcon, IconButton, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const GripIcon = createIcon({
  displayName: "GripIcon",
  viewBox: "0 0 16 16",
  path: (
    <g fill="currentColor">
      <circle cx="6" cy="3.5" r="1.25" />
      <circle cx="10" cy="3.5" r="1.25" />
      <circle cx="6" cy="8" r="1.25" />
      <circle cx="10" cy="8" r="1.25" />
      <circle cx="6" cy="12.5" r="1.25" />
      <circle cx="10" cy="12.5" r="1.25" />
    </g>
  ),
});

export interface DragHandleProps {
  onDragStart: (event: React.DragEvent<HTMLElement>) => void;
  onDragEnd: () => void;
  /** Moves the cell one place up (-1) or down (1), for keyboard use. */
  onMove: (delta: -1 | 1) => void;
}

/** Drag to reorder a cell; focused, the arrow keys move it instead. */
export const DragHandle = ({
  onDragStart,
  onDragEnd,
  onMove,
}: DragHandleProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <IconButton
      aria-label="Reorder cell"
      title="Drag to reorder, or focus and press ↑ or ↓"
      icon={<GripIcon boxSize="3.5" />}
      size="xs"
      variant="inline"
      minW="5"
      cursor="grab"
      color={colors.foreground.tertiary}
      _active={{ cursor: "grabbing" }}
      draggable
      onDragStart={onDragStart}
      onDragEnd={onDragEnd}
      onKeyDown={(event) => {
        if (event.key === "ArrowUp" || event.key === "ArrowDown") {
          event.preventDefault();
          onMove(event.key === "ArrowUp" ? -1 : 1);
        }
      }}
    />
  );
};
