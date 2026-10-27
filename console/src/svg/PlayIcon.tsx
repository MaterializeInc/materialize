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

const PlayIcon = (props: IconProps) => {
  return (
    <Icon
      width="4"
      height="4"
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/I"
      {...props}
    >
      <path
        d="M4 3.76619V12.2338C4 13.0111 4.84797 13.4912 5.5145 13.0913L12.5708 8.85749C13.2182 8.46909 13.2182 7.53091 12.5708 7.14251L5.5145 2.9087C4.84797 2.50878 4 2.9889 4 3.76619Z"
        fill="currentColor"
      />
    </Icon>
  );
};

export default PlayIcon;
