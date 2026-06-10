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

export const HamburgerIcon = forwardRef<IconProps & { color?: string }, "svg">(
  (props, ref) => {
    const { colors } = useTheme<MaterializeTheme>();

    return (
      <Icon
        viewBox="0 0 16 16"
        xmlns="http://www.w3.org/2000/svg"
        width="4"
        height="4"
        ref={ref}
        {...props}
        fill={props.color || colors.foreground.secondary}
      >
        <rect y="3" width="16" height="1.33333" rx="0.666667" />
        <rect y="7.33333" width="16" height="1.33333" rx="0.666667" />
        <rect y="11.6667" width="16" height="1.33333" rx="0.666667" />
      </Icon>
    );
  },
);
