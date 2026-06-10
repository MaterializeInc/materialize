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

const GlobeIcon = forwardRef<IconProps, "svg">((props, ref) => {
  return (
    <Icon
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      ref={ref}
      {...props}
    >
      <g clipPath="url(#clip0_299_3945)">
        <path
          d="M7.99992 14.6666C11.6818 14.6666 14.6666 11.6819 14.6666 7.99998C14.6666 4.31808 11.6818 1.33331 7.99992 1.33331C4.31802 1.33331 1.33325 4.31808 1.33325 7.99998C1.33325 11.6819 4.31802 14.6666 7.99992 14.6666Z"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M1.33325 8H14.6666"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M7.99992 1.33331C9.66744 3.15888 10.6151 5.528 10.6666 7.99998C10.6151 10.472 9.66744 12.8411 7.99992 14.6666C6.3324 12.8411 5.38475 10.472 5.33325 7.99998C5.38475 5.528 6.3324 3.15888 7.99992 1.33331Z"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </g>
      <defs>
        <clipPath id="clip0_299_3945">
          <rect width="16" height="16" fill="white" />
        </clipPath>
      </defs>
    </Icon>
  );
});

export default GlobeIcon;
