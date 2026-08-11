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

const VsCodeLogoIcon = (props: IconProps) => {
  return (
    <Icon
      width="16"
      height="16"
      viewBox="0 0 16 16"
      xmlns="http://www.w3.org/2000/svg"
      fill="#0078D4"
      {...props}
    >
      <path d="M11.5 1.5L14 3v10l-2.5 1.5L5 9.8l-2.6 2L1 11l2.6-3L1 5l1.4-.8 2.6 2 6.5-4.7zM11 5.2L7.6 8 11 10.8V5.2z" />
    </Icon>
  );
};

export default VsCodeLogoIcon;
