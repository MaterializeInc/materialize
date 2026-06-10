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

const WarningIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      ref={ref}
      color={colors.foreground.secondary}
      {...props}
    >
      <path
        d="M7.99998 14.6667C11.6819 14.6667 14.6666 11.6819 14.6666 7.99999C14.6666 4.3181 11.6819 1.33333 7.99998 1.33333C4.31808 1.33333 1.33331 4.3181 1.33331 7.99999C1.33331 11.6819 4.31808 14.6667 7.99998 14.6667Z"
        stroke="currentColor"
        strokeWidth={1.33333}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 5L8 9"
        stroke="currentColor"
        strokeWidth={1.33333}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 10.6667H8.00667"
        stroke="currentColor"
        strokeWidth={1.33333}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
});

export default WarningIcon;
