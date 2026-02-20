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

export const IndexIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      ref={ref}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      color={colors.foreground.tertiary}
      {...props}
    >
      <g clipPath="url(#clip0_14_9835)">
        <rect
          x="3"
          y="3"
          width="10"
          height="10"
          rx="2"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <rect
          x="6"
          y="6"
          width="4"
          height="4"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M6 3V1"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M3 10L1 10"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M6 15V13"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M15 10L13 10"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M10 3V1"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M3 6L1 6"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M10 15V13"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M15 6L13 6"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </g>
      <defs>
        <clipPath id="clip0_14_9835">
          <rect width="16" height="16" fill="white" />
        </clipPath>
      </defs>
    </Icon>
  );
});

export default IndexIcon;
