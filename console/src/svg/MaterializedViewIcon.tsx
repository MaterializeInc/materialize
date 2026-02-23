// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Icon, IconProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const MaterializedViewIcon = forwardRef<IconProps, "svg">(
  (props, ref) => {
    const { colors } = useTheme<MaterializeTheme>();

    return (
      <Icon
        ref={ref}
        viewBox="0 0 16 16"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        width="4"
        height="4"
        {...props}
      >
        <path
          d="M9 7L9 14"
          stroke={colors.foreground.tertiary}
          strokeLinecap="round"
        />
        <path
          d="M13 4.33333V4C13 2.89543 12.1046 2 11 2H3C1.89543 2 1 2.89543 1 4V10C1 11.1046 1.89543 12 3 12H3.27273"
          stroke={colors.foreground.tertiary}
        />
        <path
          d="M3 7H15"
          stroke={colors.foreground.tertiary}
          strokeLinejoin="round"
        />
        <path
          d="M3 10H15"
          stroke={colors.foreground.tertiary}
          strokeLinejoin="round"
        />
        <rect
          x="3"
          y="4"
          width="12"
          height="10"
          rx="2"
          stroke={colors.foreground.secondary}
        />
      </Icon>
    );
  },
);

export default MaterializedViewIcon;
