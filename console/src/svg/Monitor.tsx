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

export const MonitorIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      ref={ref}
      color={colors.foreground.secondary}
      {...props}
    >
      <g>
        <rect x="1" y="2" width="14" height="10" rx="2" stroke="currentColor" />
        <path d="M8 12V14" stroke="currentColor" strokeLinecap="round" />
        <path d="M10 14H6" stroke="currentColor" strokeLinecap="round" />
      </g>
    </Icon>
  );
});
