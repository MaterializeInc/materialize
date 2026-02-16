// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { selectAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const { defineMultiStyleConfig: defineSelectConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(selectAnatomy.keys);

export const Select = defineSelectConfig({
  baseStyle: definePartsStyle({
    icon: {
      color: "foreground.secondary",
    },
  }),
  variants: {
    borderless: definePartsStyle({
      field: {
        color: "foreground.secondary",
        fontSize: "14px",
        height: "32px",
        lineHeight: "16px",
        rounded: "8px",
        _hover: {
          cursor: "pointer",
        },
        _focus: {
          background: "background.secondary",
        },
      },
    }),
  },
});
