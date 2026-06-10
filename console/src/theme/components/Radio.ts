// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { radioAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const { defineMultiStyleConfig, definePartsStyle } =
  createMultiStyleConfigHelpers(radioAnatomy.keys);

const variantForm = definePartsStyle(({ theme }) => {
  const { colors } = theme;
  return {
    container: {
      alignItems: "start",
    },
    control: {
      marginTop: "0.2lh",
      borderColor: colors.border.primary,
      "&[data-checked]": {
        color: colors.background.primary,
        backgroundColor: colors.accent.brightPurple,
        borderColor: colors.accent.brightPurple,
      },
      "&[data-checked][data-hover]": {
        backgroundColor: colors.accent.purple,
        borderColor: colors.accent.purple,
      },
    },
    label: {
      marginInlineStart: 4,
    },
  };
});

export const Radio = defineMultiStyleConfig({
  variants: {
    form: variantForm,
  },
});
