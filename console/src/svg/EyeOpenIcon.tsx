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

const EyeOpenIcon = (props: IconProps & { color?: string }) => {
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
        d="M0.88453 9.04951C0.54171 8.52153 0.541711 7.85728 0.884531 7.3293C1.86408 5.82069 4.23165 2.85607 7.71094 2.85607C11.1902 2.85607 13.5578 5.82069 14.5374 7.3293C14.8802 7.85728 14.8802 8.52153 14.5374 9.04951C13.5578 10.5581 11.1902 13.5227 7.71094 13.5227C4.23164 13.5227 1.86408 10.5581 0.88453 9.04951Z"
        stroke={props.color || colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M7.71094 10.1894C8.81551 10.1894 9.71094 9.29398 9.71094 8.18941C9.71094 7.08484 8.81551 6.18941 7.71094 6.18941C6.60637 6.18941 5.71094 7.08484 5.71094 8.18941C5.71094 9.29398 6.60637 10.1894 7.71094 10.1894Z"
        stroke={props.color || colors.foreground.secondary}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
};

export default EyeOpenIcon;
