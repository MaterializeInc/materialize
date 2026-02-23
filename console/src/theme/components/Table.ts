// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  createMultiStyleConfigHelpers,
  HTMLChakraProps,
} from "@chakra-ui/react";

import { typographySystem } from "../typography";

const tableBorderStyle = `solid 1px ${`border.secondary`}`;

const { defineMultiStyleConfig } = createMultiStyleConfigHelpers([
  "table",
  "th",
]);

/*
  A fixed set of max widths for table cells such that a long string with no spaces inside will truncate rather than
  wrap vertically.

  Use this style in a table cell and noOfLines={1} in the Text component to handle overflowing text.
  Note: This method isn't perfect and is a limitation of how browsers handle tables. We should
  deprecate this in favor of https://github.com/MaterializeInc/console/issues/983's solution.

  Example:
    <Td {...truncateMaxWidth}>
      <Text noOfLines={1}>
        Very long texttttttt
      </Text>
    </Td>
 */
export const truncateMaxWidth: HTMLChakraProps<"td"> = {
  maxW: {
    base: "120px",
    xl: "200px",
    "2xl": "400px",
    "3xl": "800px",
    "4xl": "1200px",
  },
};

export const Table = defineMultiStyleConfig({
  baseStyle: {},
  variants: {
    rounded: {
      table: {
        borderCollapse: "separate",
        borderSpacing: 0,
        borderRadius: "xl",
        borderWidth: "1px",
      },
      tr: {
        "&:last-child": {
          td: {
            border: "none",
          },
        },
      },
      td: {
        borderBottomWidth: "1px",
      },
      th: {
        borderBottomWidth: "1px",
      },
    },
    linkable: {
      table: {
        borderCollapse: "separate",
        borderSpacing: 0,
      },
      th: {
        textTransform: "none",
        fontFamily: "body",
        color: "foreground.secondary",
        fontSize: "sm",
        fontWeight: "500",
        backgroundColor: "background.secondary",
        border: tableBorderStyle,
        borderWidth: "1px",
        borderColor: "border.secondary",
        borderX: "none",
        "&:first-of-type": {
          borderRadius: "8px 0 0 8px",
          borderLeftWidth: "1px",
          borderLeftStyle: "solid",
          borderLeftColor: "border.secondary",
        },
        "&:only-child": {
          borderRadius: "8px",
          borderWidth: "1px",
          borderStyle: "solid",
          borderColor: "border.secondary",
        },
        "&:last-child:not(:only-child)": {
          borderRadius: "0 8px 8px 0",
          borderRightWidth: "1px",
          borderRightStyle: "solid",
          borderRightColor: "border.secondary",
        },
      },
      tr: {
        _hover: {
          bg: "background.secondary",
        },
      },
      td: {
        lineHeight: "18px",
        borderBottom: tableBorderStyle,
        borderBottomWidth: "1px",
        borderBottomColor: "border.primary",
      },
    },
    standalone: {
      table: {
        borderCollapse: "separate",
        borderSpacing: 0,
      },
      th: {
        textTransform: "none",
        fontFamily: "body",
        color: "foreground.secondary",
        fontSize: "sm",
        fontWeight: "500",
        backgroundColor: "background.secondary",
        border: tableBorderStyle,
        borderWidth: "1px",
        borderColor: "border.secondary",
        borderX: "none",
        "&:first-of-type": {
          borderRadius: "8px 0 0 8px",
          borderLeftWidth: "1px",
          borderLeftStyle: "solid",
          borderLeftColor: "border.secondary",
        },
        "&:only-child": {
          borderRadius: "8px",
          borderWidth: "1px",
          borderStyle: "solid",
          borderColor: "border.secondary",
        },
        "&:last-child:not(:only-child)": {
          borderRadius: "0 8px 8px 0",
          borderRightWidth: "1px",
          borderRightStyle: "solid",
          borderRightColor: "border.secondary",
        },
        "&[data-is-numeric]": {
          textAlign: "end",
        },
      },
      td: {
        borderBottom: tableBorderStyle,
        borderBottomWidth: "1px",
        borderBottomColor: "border.primary",
        "&[data-is-numeric]": {
          textAlign: "end",
        },
      },
    },
    shell: {
      table: {
        border: 0,
      },
      th: {
        fontWeight: "500",
        color: "foreground.secondary",
        borderWidth: "1px 0px",
        borderStyle: "solid",
        backgroundColor: "background.secondary",
        textTransform: "none",
        borderColor: "border.secondary",
      },
      tr: {
        transition: "background 0.1s ease-out",
        _hover: {
          backgroundColor: "background.secondary",
        },
      },
      td: {
        borderBottomWidth: "0",
        height: "auto",
        verticalAlign: "baseline",
      },
    },
    borderless: {
      table: {
        borderCollapse: "separate",
        borderSpacing: 0,
      },
      td: {
        alignItems: "center",
        borderBottomWidth: "1px",
        borderColor: "border.primary",
        color: "foreground.primary",
      },

      th: {
        ...typographySystem["text-ui-med"],
        borderBottomWidth: "1px",
        borderColor: "border.secondary",
        color: "foreground.secondary",
        textTransform: "none",
        whiteSpace: "nowrap",
      },
    },
    tooltip: {
      table: {
        borderCollapse: "separate",
        borderSpacing: 0,
      },
      th: {
        textTransform: "none",
        fontFamily: "body",
        border: "none",
        fontWeight: "400",
        textStyle: "text-ui-reg",
        "&[data-is-numeric]": {
          textAlign: "end",
        },
        paddingInlineStart: 3,
        paddingInlineEnd: 3,
        "&:first-of-type": {
          paddingInlineStart: 0,
        },
        "&:last-of-type": {
          paddingInlineEnd: 0,
        },
      },
      td: {
        textStyle: "text-ui-reg",
        "&[data-is-numeric]": {
          textAlign: "end",
        },
        paddingInlineStart: 3,
        paddingInlineEnd: 3,
        "&:first-of-type": {
          paddingInlineStart: 0,
        },
        "&:last-of-type": {
          paddingInlineEnd: 0,
        },
      },
    },
  },
  sizes: {
    md: {
      th: {
        height: "32px",
        px: "4",
        py: "0",
        lineHeight: "4",
        fontSize: "sm",
      },
      td: {
        height: "40px",
        px: "4",
        py: "0",
        lineHeight: "4",
        fontSize: "sm",
      },
    },
  },
});
