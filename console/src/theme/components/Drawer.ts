// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { drawerAnatomy as parts } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/styled-system";
import { mode, StyleFunctionProps } from "@chakra-ui/theme-tools";

import colors from "../colors";

const { defineMultiStyleConfig } = createMultiStyleConfigHelpers(parts.keys);

export const Drawer = defineMultiStyleConfig({
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
  sizes: {
    xs: {
      dialog: { maxW: "xs" },
    },
    sm: {
      dialog: { maxW: "sm" },
    },
    md: {
      dialog: { maxW: "md" },
    },
    lg: {
      dialog: { maxW: "lg" },
    },
    xl: {
      dialog: { maxW: "xl" },
    },
    full: {
      dialog: { maxW: "100vw", h: "100vh" },
    },
  },
});
