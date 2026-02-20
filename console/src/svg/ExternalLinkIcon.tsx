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

const ExternalLinkIcon = (props: IconProps) => {
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
        d="M7 4H4C2.89543 4 2 4.89543 2 6V12C2 13.1046 2.89543 14 4 14H10C11.1046 14 12 13.1046 12 12V9"
        stroke="currentColor"
        strokeLinecap="round"
        strokeWidth="1.33px"
      />
      <path
        d="M10 2H14V6"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.33px"
      />
      <path
        d="M14 2L8 8"
        stroke="currentColor"
        strokeLinecap="round"
        strokeWidth="1.33px"
      />
    </Icon>
  );
};

export default ExternalLinkIcon;
