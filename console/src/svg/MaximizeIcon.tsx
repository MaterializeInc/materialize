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

const MaximizeIcon = forwardRef<IconProps, "svg">((props, ref) => {
  return (
    <Icon
      ref={ref}
      xmlns="http://www.w3.org/2000/svg"
      width="4"
      height="4"
      viewBox="0 0 16 16"
      fill="none"
      {...props}
    >
      <path
        d="M9.99988 2.00003H13.9999V6.00003"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M5.99988 14H1.99988V10"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M13.9998 2.00003L9.33313 6.6667"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M1.99988 14L6.66654 9.33337"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </Icon>
  );
});

export default MaximizeIcon;
