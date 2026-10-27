// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

const SearchIcon = (props: { color: string }) => (
  <svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <circle cx="7" cy="7" r="5" stroke={props.color} />
    <path
      d="M11.3536 10.6464L11 10.2929L10.2929 11L10.6464 11.3536L11.3536 10.6464ZM13.6464 14.3536C13.8417 14.5488 14.1583 14.5488 14.3536 14.3536C14.5488 14.1583 14.5488 13.8417 14.3536 13.6464L13.6464 14.3536ZM10.6464 11.3536L13.6464 14.3536L14.3536 13.6464L11.3536 10.6464L10.6464 11.3536Z"
      fill="#66626A"
    />
  </svg>
);

export default SearchIcon;
