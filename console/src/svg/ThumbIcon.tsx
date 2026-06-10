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

const ThumbIcon = forwardRef<IconProps, "svg">((props, ref) => {
  return (
    <Icon
      ref={ref}
      xmlns="http://www.w3.org/2000/svg"
      width="5"
      height="4"
      viewBox="0 0 20 18"
      {...props}
    >
      <path
        d="M11.11 3.72L10.54 6.61C10.42 7.2 10.58 7.81 10.96 8.27C11.34 8.73 11.9 9 12.5 9H18V10.08L15.43 16H7.34C7.25062 15.9975 7.1656 15.9609 7.10237 15.8976C7.03915 15.8344 7.00252 15.7494 7 15.66V7.82L11.11 3.72ZM12 0L5.59 6.41C5.21 6.79 5 7.3 5 7.83V15.66C5 16.95 6.05 18 7.34 18H15.44C16.15 18 16.8 17.63 17.16 17.03L19.83 10.88C19.94 10.63 20 10.36 20 10.08V9C20 7.9 19.1 7 18 7H12.5L13.42 2.35C13.47 2.13 13.44 1.89 13.34 1.69C13.1126 1.23961 12.8156 0.82789 12.46 0.47L12 0ZM2 7H0V18H2C2.55 18 3 17.55 3 17V8C3 7.45 2.55 7 2 7Z"
        fill="currentColor"
      />
    </Icon>
  );
});

export default ThumbIcon;
