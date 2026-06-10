// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { stepperAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const { defineMultiStyleConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(stepperAnatomy.keys);

const variantModalHeader = definePartsStyle(({ theme }) => {
  const { colors, textStyles, space } = theme;
  return {
    indicator: {
      width: 3,
      height: 3,
      "&[data-status=active]": {
        borderColor: colors.accent.brightPurple,
      },
      "&[data-status=complete]": {
        bg: colors.border.secondary,
      },
    },
    title: {
      ...textStyles["text-ui-med"],
      whiteSpace: "nowrap",
    },
    separator: {
      minWidth: space["16"],
      "&[data-status=complete]": {
        bg: colors.border.secondary,
      },
    },
  };
});

export const Stepper = defineMultiStyleConfig({
  variants: {
    "modal-header": variantModalHeader,
  },
});
