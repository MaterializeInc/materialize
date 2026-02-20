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

export const TableIcon = forwardRef<IconProps, "svg">((props, ref) => {
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
      <path d="M1 6H15" stroke="currentColor" strokeLinejoin="round" />
      <path d="M1 10H15" stroke="currentColor" strokeLinejoin="round" />
      <path d="M8 6L8 14" stroke="currentColor" strokeLinejoin="round" />
      <rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" />
    </Icon>
  );
});

export default TableIcon;
