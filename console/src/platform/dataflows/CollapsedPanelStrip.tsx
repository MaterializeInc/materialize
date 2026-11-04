// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, type ChakraProps, Text } from "@chakra-ui/react";
import React from "react";

export const COLLAPSED_STRIP_WIDTH = "28px";

export interface CollapsedPanelStripProps extends ChakraProps {
  /** Vertical text shown on the strip. */
  label: string;
  /** Accessible name, naming the action (e.g. "Expand LIR panel"). */
  ariaLabel: string;
  chevron: React.ReactNode;
  onClick: () => void;
}

// The whole strip is the click target: a 12px chevron alone is easy to miss.
export const CollapsedPanelStrip = ({
  label,
  ariaLabel,
  chevron,
  onClick,
  ...props
}: CollapsedPanelStripProps) => (
  <chakra.button
    type="button"
    aria-label={ariaLabel}
    title={ariaLabel}
    onClick={onClick}
    width={COLLAPSED_STRIP_WIDTH}
    flexShrink={0}
    display="flex"
    flexDirection="column"
    alignItems="center"
    gap={2}
    pt={2}
    background="background.primary"
    color="foreground.secondary"
    _hover={{ background: "background.secondary" }}
    {...props}
  >
    {chevron}
    <Text
      as="span"
      fontSize="xs"
      fontWeight="600"
      // Reads top to bottom, the direction vertical-rl lays out text in.
      sx={{ writingMode: "vertical-rl" }}
    >
      {label}
    </Text>
  </chakra.button>
);
