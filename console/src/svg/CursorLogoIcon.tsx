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

const CursorLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="16"
      height="16"
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.2"
      {...props}
    >
      <path
        d="M8 1.5l5.6 3.25v6.5L8 14.5l-5.6-3.25v-6.5L8 1.5z"
        strokeLinejoin="round"
      />
      <path d="M2.4 4.75L8 8m0 0l5.6-3.25M8 8v6.5" strokeLinejoin="round" />
    </Icon>
  );
};

export default CursorLogoIcon;
