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

const AdminIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      height="4"
      width="4"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      ref={ref}
      color={colors.foreground.tertiary}
      {...props}
    >
      <g clipPath="url(#clip0_54_11642)">
        <path
          d="M7.6013 1.14498L1.76797 3.26619C1.30691 3.43385 0.994139 3.87256 1.03194 4.36169C1.44585 9.71716 5.77949 15 8 15C10.2205 15 14.5541 9.71716 14.9681 4.36169C15.0059 3.87256 14.6931 3.43385 14.232 3.26619L8.3987 1.14498C8.14115 1.05133 7.85885 1.05133 7.6013 1.14498Z"
          stroke="currentColor"
          strokeWidth="1.33"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <circle
          cx="8.00033"
          cy="5.83333"
          r="2.33333"
          stroke="currentColor"
          strokeWidth="1.33"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M4.5 11.5007C5.35981 10.7846 6.6093 10.334 8 10.334C9.3907 10.334 10.6402 10.7846 11.5 11.5007"
          stroke="currentColor"
          strokeWidth="1.33"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </g>
      <defs>
        <clipPath id="clip0_54_11642">
          <rect width="16" height="16" fill="white" />
        </clipPath>
      </defs>
    </Icon>
  );
});

export default AdminIcon;
