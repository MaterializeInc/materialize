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

const ZoomInIcon = (props: IconProps) => {
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
      <circle cx="7" cy="7" r="5" stroke={colors.foreground.secondary} />
      <path
        d="M11.3536 10.6464L11 10.2929L10.2929 11L10.6464 11.3536L11.3536 10.6464ZM13.6464 14.3536C13.8417 14.5488 14.1583 14.5488 14.3536 14.3536C14.5488 14.1583 14.5488 13.8417 14.3536 13.6464L13.6464 14.3536ZM10.6464 11.3536L13.6464 14.3536L14.3536 13.6464L11.3536 10.6464L10.6464 11.3536Z"
        fill={colors.foreground.secondary}
      />
      <path
        d="M5 7H9"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
      <path
        d="M7 9L7 5"
        stroke={colors.foreground.secondary}
        strokeLinecap="round"
      />
    </Icon>
  );
};

export default ZoomInIcon;
