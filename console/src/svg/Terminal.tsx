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

const TerminalIcon = (props: IconProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <rect
        x="1"
        y="2"
        width="14"
        height="12"
        rx="2"
        stroke={colors.foreground.secondary}
      />
      <path
        d="M5 6L7 8L5 10"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 10H11"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
};

export default TerminalIcon;
