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

const ViewIcon = (props: IconProps) => {
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
        d="M13 3.64785V3C13 2.44772 12.5523 2 12 2H2C1.44772 2 1 2.44772 1 3V11C1 11.5523 1.44771 12 2 12H2.81583"
        stroke={colors.foreground.tertiary}
      />
      <path
        d="M3 7H15"
        stroke={colors.foreground.tertiary}
        strokeLinejoin="round"
      />
      <rect
        x="3"
        y="4"
        width="12"
        height="10"
        rx="1"
        stroke={colors.foreground.secondary}
      />
    </Icon>
  );
};

export default ViewIcon;
