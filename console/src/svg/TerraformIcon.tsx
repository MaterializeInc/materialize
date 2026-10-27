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

const TerraformIcon = (props: IconProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Icon
      width="12"
      height="12"
      viewBox="0 0 48 48"
      xmlns="http://www.w3.org/2000/svg"
      color={colors.foreground.tertiary}
      {...props}
    >
      <path d="M17.5 8L30.5 15.5V30.5L17.5 23V8Z" fill="currentColor" />
      <path d="M32.5 15.5L45.5 8V23L32.5 30.5V15.5Z" fill="currentColor" />
      <path d="M2.5 15.5L15.5 8V23L2.5 30.5V15.5Z" fill="currentColor" />
      <path d="M17.5 25L30.5 32.5V47.5L17.5 40V25Z" fill="currentColor" />
    </Icon>
  );
};

export default TerraformIcon;
