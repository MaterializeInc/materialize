// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { switchAnatomy } from "@chakra-ui/anatomy";
import { createMultiStyleConfigHelpers } from "@chakra-ui/react";

const {
  defineMultiStyleConfig: defineSwitchConfig,
  definePartsStyle: defineSwitchPartsStyle,
} = createMultiStyleConfigHelpers(switchAnatomy.keys);

export const Switch = defineSwitchConfig({
  defaultProps: {
    size: "sm",
  },
  baseStyle: defineSwitchPartsStyle({
    thumb: {
      backgroundBlendMode: "multiply, normal",
      background: "background.primary",
      shadow:
        "0px 0px 0.5px rgba(0, 0, 0, 0.40), 0px 0.5px 2px rgba(0, 0, 0, 0.16)",
      _checked: {
        background: "white",
      },
    },
    track: {
      backgroundColor: "border.secondary",
      shadow:
        "inset 0px 0px 0.5px rgba(0, 0, 0, 0.16), inset 0px 0px 2px rgba(0, 0, 0, 0.08)",
      _checked: {
        backgroundColor: "accent.brightPurple",
        shadow:
          "inset 0px 0px 0.5px rgba(0, 0, 0, 0.16), inset 0px 0px 2px rgba(0, 0, 0, 0.12), 0 0 4px 0 rgba(90, 52, 302, 0.16)",
      },
      _focus: {
        shadow: "0 0 0 2px rgba(255, 255, 255, 1)",
      },
    },
  }),
});
