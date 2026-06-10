// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { modalAnatomy as parts } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/styled-system";
import { mode, StyleFunctionProps } from "@chakra-ui/theme-tools";

const { defineMultiStyleConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(parts.keys);

import colors from "../colors";

const DEFAULT_BORDER_RADIUS = "xl";

function getSize(value: string) {
  return {
    dialog: {
      maxW: { base: "100vw", lg: value },
      minH: { base: "100dvh", lg: "inherit" },
      my: { base: "0", lg: "16" },
      borderRadius: { base: "0", lg: DEFAULT_BORDER_RADIUS },
    },
  };
}

const sizes = {
  xs: getSize("xs"),
  sm: getSize("sm"),
  md: getSize("md"),
  lg: getSize("lg"),
  xl: getSize("xl"),
  "2xl": getSize("2xl"),
  "3xl": getSize("3xl"),
  "4xl": getSize("4xl"),
  "5xl": getSize("5xl"),
  "6xl": getSize("6xl"),
  full: definePartsStyle({
    dialog: {
      maxW: "100vw",
      minH: "100dvh",
      my: "0",
      borderRadius: "0",
    },
  }),
};

export const Modal = defineMultiStyleConfig({
  baseStyle: (props: StyleFunctionProps) => ({
    overlay: {
      background: "rgba(0, 0, 0, 0.5)",
    },
    header: {
      border: "0",
      fontSize: "md",
      lineHeight: "16px",
      fontWeight: "500",
      pb: "0",
    },
    dialog: {
      borderRadius: DEFAULT_BORDER_RADIUS,
      backgroundColor: "background.primary",
      shadows: "shadows.level4",
    },
    body: {
      px: "24px",
      py: "16px",
    },
    footer: {
      border: "0",
      borderTop: "1px solid",
      borderTopColor: mode(colors.gray[100], colors.gray[700])(props),
      fontWeight: "400",
    },
    closeButton: {
      right: "2",
      color: "foreground.secondary",
    },
  }),
  defaultProps: {
    size: "md",
  },
  sizes,
  variants: {
    "shrink-to-fit": {
      dialog: {
        maxWidth: "none",
        width: "auto",
        borderRadius: "lg",
      },
    },
  },
});
