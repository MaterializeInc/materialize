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

const WindsurfLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="16"
      height="16"
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      fill="#0B928F"
      {...props}
    >
      <path d="M3 12c3-1 7-1 10 0-2.5 2-7.5 2-10 0zM4 10c2.5-6 6-8 9-8.5C11 4 9.5 7 8.8 10c-1.6-.3-3.2-.3-4.8 0z" />
    </Icon>
  );
};

export default WindsurfLogoIcon;
