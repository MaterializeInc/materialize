// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Icon, IconProps } from "@chakra-ui/react";
import React from "react";

export const AwsAuroraLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="32"
      height="32"
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <g>
        <path
          d="M2.00195 24.885L5.32065 28.7742L5.61906 28.4198V3.54328L5.32065 3.16736L2.00195 7.05519V24.885Z"
          fill="#1A476F"
        />
        <path
          d="M5.32227 28.7742L11.6636 31.9446L11.9273 31.5202L11.9317 0.33218L11.668 0L5.32227 3.16025V28.7742Z"
          fill="#1F5B98"
        />
        <path
          d="M30.3491 7.05519L27.0295 3.16736L26.6582 3.28495L26.7319 28.4568L27.0295 28.7742L30.3491 24.8854V7.05519Z"
          fill="#2D72B8"
        />
        <path
          d="M20.6873 31.9446L27.0286 28.7742V3.16025L20.6829 0L20.3828 0.406035L20.3871 31.4832L20.6873 31.9446Z"
          fill="#5294CF"
        />
        <path d="M11.668 0H20.6838V31.9451H11.668V0Z" fill="#2D72B8" />
      </g>
    </Icon>
  );
};
