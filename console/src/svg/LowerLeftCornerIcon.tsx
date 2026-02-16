// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Icon, IconProps } from "@chakra-ui/react";
import React from "react";

const LowerLeftCornerIcon = forwardRef<IconProps, "svg">((props, ref) => (
  <Icon
    height="4"
    width="4"
    viewBox="0 0 16 16"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    ref={ref}
    {...props}
  >
    <g id="Frame 536">
      <path
        id="Vector 148"
        d="M4 1V6C4 7.65685 5.34315 9 7 9H12"
        stroke="currentColor"
        strokeLinecap="round"
      />
    </g>
  </Icon>
));

export default LowerLeftCornerIcon;
