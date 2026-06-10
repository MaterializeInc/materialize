// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { checkboxAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const {
  definePartsStyle: defineCheckboxPartsStyle,
  defineMultiStyleConfig: defineCheckboxStyleConfig,
} = createMultiStyleConfigHelpers(checkboxAnatomy.keys);

const CHECKED_STYLES = {
  backgroundColor: "accent.brightPurple",
  border: "none",
};

const FOCUSED_STYLES = {
  outline: "none",
  borderColor: "accent.brightPurple",
  boxShadow: "input.focus",
};

export const Checkbox = defineCheckboxStyleConfig({
  baseStyle: defineCheckboxPartsStyle({
    control: {
      backgroundColor: "background.primary",
      borderRadius: "base",
      boxShadow: "level1",
      borderWidth: "1px",
      borderColor: "border.secondary",

      _focus: {
        ...FOCUSED_STYLES,
      },

      _checked: {
        ...CHECKED_STYLES,
        _hover: {
          ...CHECKED_STYLES,
        },
        _focus: {
          ...FOCUSED_STYLES,
        },
      },
    },
  }),
});
