// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { typographySystem } from "../typography";

export const Accordion = {
  defaultProps: {
    variant: "borderless",
  },
  variants: {
    borderless: {
      container: {
        border: "none",
        p: "0",
      },
      button: {
        ...typographySystem["text-ui-med"],
        color: "foreground.primary",
        lineHeight: "16px",

        px: "4",
        py: "2",
        _hover: {
          background: "transparent",
        },
      },
      icon: {
        width: "16px",
        height: "16px",
      },
      panel: {
        px: "4",
        py: "2",
      },
    },
  },
};
