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

const BookOpenIcon = (props: IconProps) => (
  <Icon
    width="4"
    height="4"
    viewBox="0 0 16 16"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <path
      d="M1.33325 3C1.33325 2.44772 1.78097 2 2.33325 2H5.33325C6.0405 2 6.71877 2.28095 7.21887 2.78105C7.71897 3.28115 7.99992 3.95942 7.99992 4.66667V14C7.99992 13.4696 7.7892 12.9609 7.41413 12.5858C7.03906 12.2107 6.53035 12 5.99992 12H2.33325C1.78097 12 1.33325 11.5523 1.33325 11V3Z"
      stroke="currentColor"
      strokeLinejoin="round"
    />
    <path
      d="M14.6667 3C14.6667 2.44772 14.219 2 13.6667 2H10.6667C9.95942 2 9.28115 2.28095 8.78105 2.78105C8.28095 3.28115 8 3.95942 8 4.66667V14C8 13.4696 8.21071 12.9609 8.58579 12.5858C8.96086 12.2107 9.46957 12 10 12H13.6667C14.219 12 14.6667 11.5523 14.6667 11V3Z"
      stroke="currentColor"
      strokeLinejoin="round"
    />
  </Icon>
);

export default BookOpenIcon;
