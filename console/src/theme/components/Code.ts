// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { defineStyleConfig } from "@chakra-ui/react";

export const Code = defineStyleConfig({
  defaultProps: {
    variant: "shell",
  },
  variants: {
    shell: {
      lineHeight: 6,
      padding: 0,
    },
    "inline-syntax": {
      boxSizing: "border-box",
      textStyle: "monospace",
      lineHeight: "20px",
      color: "foreground.secondary",
      border: "1px solid",
      borderColor: "border.primary",
      backgroundColor: "background.secondary",
      borderRadius: "md",
      px: 1,
    },
  },
});
