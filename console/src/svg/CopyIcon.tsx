// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const CopyIcon = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        d="M10 4V3C10 1.89543 9.10457 1 8 1H3C1.89543 1 1 1.89543 1 3V8C1 9.10457 1.89543 10 3 10H4"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
      <rect
        x="6"
        y="6"
        width="9"
        height="9"
        rx="2"
        stroke={colors.foreground.secondary}
      />
    </svg>
  );
};

export default CopyIcon;
