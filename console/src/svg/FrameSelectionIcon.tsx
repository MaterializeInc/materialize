// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Icon, IconProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const FrameSelectionIcon = (props: IconProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      {...props}
    >
      <path
        d="M2 6V4C2 2.89543 2.89543 2 4 2H6"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
      <path
        d="M2 10V12C2 13.1046 2.89543 14 4 14H6"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
      <path
        d="M14 6V4C14 2.89543 13.1046 2 12 2H10"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
      <path
        d="M14 10V12C14 13.1046 13.1046 14 12 14H10"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
    </Icon>
  );
};

export default FrameSelectionIcon;
