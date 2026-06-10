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

export const MsftLogoIcon = (props: IconProps) => {
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
          fillRule="evenodd"
          clipRule="evenodd"
          d="M0 14.5455H14.5455V0H0V14.5455Z"
          fill="#F25022"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M17.4551 14.5455H32.0005V0H17.4551V14.5455Z"
          fill="#7FBA00"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M0 32H14.8837V17.4546H0V32Z"
          fill="#00A4EF"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M17.4551 32H32.0005V17.4546H17.4551V32Z"
          fill="#FFB900"
        />
      </g>
    </Icon>
  );
};
