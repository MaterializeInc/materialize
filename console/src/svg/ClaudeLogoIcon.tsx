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

const ClaudeLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="16"
      height="16"
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      fill="#D97757"
      {...props}
    >
      <path d="M8 1l1.2 4.2L13 3.5l-2.6 3.4L14.5 8l-4.1 1.1L13 12.5l-3.8-1.7L8 15l-1.2-4.2L3 12.5l2.6-3.4L1.5 8l4.1-1.1L3 3.5l3.8 1.7L8 1z" />
    </Icon>
  );
};

export default ClaudeLogoIcon;
