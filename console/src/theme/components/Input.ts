// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { inputAnatomy } from "@chakra-ui/anatomy";
import {
  createMultiStyleConfigHelpers,
  defineStyle,
  defineStyleConfig,
} from "@chakra-ui/react";

import { MaterializeTheme } from "..";

export const INPUT_ERROR_STYLES = {
  borderRadius: "lg",
  borderColor: "accent.red",
  boxShadow: "input.error",
  _focus: {
    backgroundColor: "background.primary",
  },
};

const {
  definePartsStyle: defineInputPartsStyle,
  defineMultiStyleConfig: defineInputStyleConfig,
} = createMultiStyleConfigHelpers(inputAnatomy.keys);

export const Input = defineInputStyleConfig({
  baseStyle: defineInputPartsStyle({
    field: {
      padding: "8px",
      fontSize: "14px",
      lineHeight: "16px",
      width: "100%",
      backgroundColor: "background.primary",
      borderWidth: "1px",
      borderColor: "border.secondary",
      boxShadow: "levelInset1",
      transition: "box-shadow 50ms ease-out",
      _hover: {
        cursor: "pointer",
      },
      _invalid: {
        ...INPUT_ERROR_STYLES,
      },
    },

    addon: {
      borderWidth: "1px",
      backgroundColor: "background.primary",
      borderColor: "border.primary",
      borderRadius: "full",
    },
  }),
  defaultProps: {
    variant: "default",
    size: "sm",
  },
  variants: {
    default: defineInputPartsStyle({
      field: {
        borderRadius: "lg",
        _focus: {
          backgroundColor: "background.primary",
          borderColor: "accent.brightPurple",
          boxShadow: "input.focus",
        },
      },
    }),
    // TODO: Remove this variant in favor of the _invalid selector https://github.com/MaterializeInc/console/issues/1428
    error: defineInputPartsStyle({
      field: {
        ...INPUT_ERROR_STYLES,
      },
    }),
    focused: defineInputPartsStyle({
      field: {
        borderRadius: "lg",
        backgroundColor: "background.primary",
        borderColor: "accent.brightPurple",
        boxShadow: "input.focus",
      },
    }),
  },
});

export const Textarea = defineStyleConfig({
  baseStyle: defineStyle({
    ...Input.baseStyle?.field,
  }),
  variants: {
    default: defineStyle(Input.variants?.default?.field ?? {}),
    error: defineStyle(Input.variants?.error?.field ?? {}),
    focused: defineStyle(Input.variants?.focused?.field ?? {}),
  },
  defaultProps: {
    variant: "default",
    size: "sm",
  },
});

// We need to duplicate these styles since Stripe's SDK doesn't allow us to use our own
// components. This helper function tries to deduplicate these styles as much as possible.
export const buildStripeInputStyles = (theme: MaterializeTheme) => {
  return {
    ".Input": {
      // Can use null-assertion since all styles are statically defined in Input.
      padding: Input.baseStyle!.field.padding,
      fontSize: Input.baseStyle!.field.fontSize,
      lineHeight: Input.baseStyle!.field.lineHeight,
      borderWidth: Input.baseStyle!.field.borderWidth,
      borderColor: theme.colors.border.secondary,
      boxShadow: theme.shadows.levelInset1,
      transition: Input.baseStyle!.field.transition,
      backgroundColor: theme.colors.background.primary,
      borderStyle: "solid",
    },
    ".Input:focus": {
      backgroundColor: theme.colors.background.primary,
      borderColor: theme.colors.accent.brightPurple,
      boxShadow: theme.shadows.input.focus,
      outline: "none",
    },
    ".Input--invalid": {
      borderColor: theme.colors.accent.red,
      boxShadow: theme.shadows.input.error,
    },
    ".Input--invalid:focus": {
      backgroundColor: theme.colors.background.primary,
    },
  };
};
