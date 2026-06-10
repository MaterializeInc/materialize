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

export const GalleryIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      ref={ref}
      color={colors.foreground.tertiary}
      {...props}
    >
      <svg
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <rect
          x="2"
          y="2"
          width="4"
          height="4"
          stroke="currentColor"
          strokeWidth="1.33333"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <rect
          x="2"
          y="10"
          width="4"
          height="4"
          stroke="currentColor"
          strokeWidth="1.33333"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <rect
          x="10"
          y="2"
          width="4"
          height="4"
          stroke="currentColor"
          strokeWidth="1.33333"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <rect
          x="10"
          y="10"
          width="4"
          height="4"
          stroke="currentColor"
          strokeWidth="1.33333"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </Icon>
  );
});

export default GalleryIcon;
