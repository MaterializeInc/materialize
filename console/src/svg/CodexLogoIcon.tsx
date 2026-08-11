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

const CodexLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="16"
      height="16"
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      stroke="#10A37F"
      strokeWidth="1.2"
      {...props}
    >
      <path d="M8 2l5.2 3v6L8 14l-5.2-3V5L8 2z" strokeLinejoin="round" />
      <path
        d="M8 5.2L10.4 6.6v2.8L8 10.8 5.6 9.4V6.6L8 5.2z"
        strokeLinejoin="round"
      />
    </Icon>
  );
};

export default CodexLogoIcon;
