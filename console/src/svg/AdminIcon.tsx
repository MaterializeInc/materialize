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

export const AdminShieldIcon = forwardRef<IconProps, "svg">((props, ref) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Icon
      width="4"
      height="4"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      ref={ref}
      color={colors.foreground.secondary}
      {...props}
    >
      <path
        d="M7.65826 2.12427L2.65826 3.94245C2.26307 4.08616 1.99498 4.46219 2.02738 4.88145C2.38216 9.47185 6.09671 14 8 14C9.90329 14 13.6178 9.47185 13.9726 4.88145C14.005 4.46219 13.7369 4.08616 13.3417 3.94245L8.34174 2.12427C8.12099 2.04399 7.87901 2.04399 7.65826 2.12427Z"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle
        cx="8"
        cy="7"
        r="1.5"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M5 11C5.73698 10.3863 6.80797 10 8 10C9.19203 10 10.263 10.3863 11 11"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
});
