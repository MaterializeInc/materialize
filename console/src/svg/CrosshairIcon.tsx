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

const CrosshairIcon = (props: IconProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Icon
      width="4"
      height="4"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <circle
        cx="8"
        cy="8"
        r="5"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 5V1"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 15V11"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M5 8H1"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M15 8L11 8"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
};

export default CrosshairIcon;
