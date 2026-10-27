// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StyleFunctionProps, theme } from "@chakra-ui/react";

import { shadows } from "../colors";

const outline = {
  transition: "all 0.1s cubic-bezier(0.4, 0, 0.2, 1)",
  _hover: {
    backgroundColor: "background.secondary",
  },
};

export const Button = {
  baseStyle: {
    borderRadius: "lg",
    useSelect: "none",
    fontSize: "14px",
    lineHeight: "16px",
    fontWeight: 500,
    _hover: {
      cursor: "pointer",
      _disabled: {
        cursor: "not-allowed",
      },
    },
  },
  variants: {
    borderless: {
      color: "foreground.secondary",
      fontSize: "14px",
      lineHeight: "16px",
      fontWeight: 500,
      backgroundColor: "background.primary",
      transition: "all 0.1s cubic-bezier(0.4, 0, 0.2, 1)",
      _hover: {
        backgroundColor: "background.secondary",
      },
    },
    secondary: {
      color: "foreground.primary",
      backgroundColor: "background.primary",
      borderWidth: "1px",
      borderColor: "border.secondary",
      shadow: shadows.light.level1,
      transition: "all 0.1s cubic-bezier(0.4, 0, 0.2, 1)",
      _hover: {
        shadow: "none",
        backgroundColor: "background.secondary",
      },
    },
    "text-only": {
      color: "foreground.primary",
      fontSize: "14px",
      lineHeight: "16px",
      fontWeight: 500,
      backgroundColor: "transparent",
    },
    primary: {
      color: "foreground.primaryButtonLabel",
      fontSize: "14px",
      lineHeight: "16px",
      fontWeight: 500,
      backgroundColor: "accent.purple",
      shadow: "shadows.level1",
      transition: "all 0.1s cubic-bezier(0.4, 0, 0.2, 1)",
      _hover: {
        shadow: "none",
        _disabled: {
          backgroundColor: "accent.purple",
        },
      },
    },
    tertiary: {
      color: "foreground.primary",
      backgroundColor: "background.tertiary",
      borderColor: "border.secondary",
      fontSize: "14px",
      lineHeight: "16px",

      borderWidth: "1px",
      shadow: shadows.light.level2,
      _hover: {
        shadow: "none",
        _disabled: {
          backgroundColor: "background.tertiary",
        },
      },
    },
    outline,
    inline: {
      borderRadius: "0",
      _hover: {
        backgroundColor: "background.secondary",
      },
    },
    card: (props: StyleFunctionProps) => ({
      ...theme.components.Button.variants?.outline(props),
      ...outline,
      height: "auto",
      display: "flex",
      padding: "4",
      gap: "4",
      whiteSpace: "wrap",
    }),
  },
};
