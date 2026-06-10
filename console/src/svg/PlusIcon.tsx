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

export const PlusIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Icon
      ref={ref}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      color={colors.foreground.secondary}
      {...props}
    >
      <path d="M8 3V13" stroke="currentColor" strokeLinecap="round" />
      <path d="M13 8L3 8" stroke="currentColor" strokeLinecap="round" />
    </Icon>
  );
});
