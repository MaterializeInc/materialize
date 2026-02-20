// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { popoverAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";
import { ModifierArguments } from "@popperjs/core";
import maxSize from "popper-max-size-modifier";

const applyMaxSizeModifier = {
  name: "applyMaxSize" as const,
  enabled: true,
  phase: "beforeWrite" as const,
  requires: ["maxSize"],
  fn({ state }: ModifierArguments<Record<string, never>>) {
    const { width, height } = state.modifiersData.maxSize;

    state.styles.popper = {
      ...state.styles.popper,
      maxWidth: `${width}px`,
      maxHeight: `${height}px`,
    };
  },
};

/**
 * We use popper-max-size-modifier to set a max size such that the popover
 * doesn't overflow the viewport. This library is deprecated, but since
 * Chakra uses it internally, we use it here as well. There are plans
 * for Chakra to move to Floating-UI, Popper.js's successor.
 */
export const viewportOverflowModifier = [maxSize, applyMaxSizeModifier];

const {
  definePartsStyle: definePopoverPartsStyle,
  defineMultiStyleConfig: definePopoverStyleConfig,
} = createMultiStyleConfigHelpers(popoverAnatomy.keys);

export const Popover = definePopoverStyleConfig({
  variants: {
    dropdown: definePopoverPartsStyle({
      popper: {
        // So that the popover doesn't overflow the viewport
        overflowY: "auto",
        borderRadius: "lg",
        shadow: "level3",
      },

      content: {
        width: "auto",
        borderRadius: "lg",
        backgroundColor: "background.primary",
      },
    }),
  },
});
