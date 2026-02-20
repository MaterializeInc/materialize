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

const SlashIcon = (props: IconProps) => {
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
        d="M5 16.875L11 1.125"
        stroke={colors.border.secondary}
        strokeWidth="1.33333"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
};

export default SlashIcon;
