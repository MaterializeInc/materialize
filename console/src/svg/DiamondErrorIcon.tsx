// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

const DiamondErrorIcon = () => (
  <svg
    width="16"
    height="16"
    viewBox="0 0 16 16"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <g clipPath="url(#clip0_1037_11752)">
      <path
        d="M1.70711 7.29289L7.29289 1.70711C7.68342 1.31658 8.31658 1.31658 8.70711 1.70711L14.2929 7.29289C14.6834 7.68342 14.6834 8.31658 14.2929 8.70711L8.70711 14.2929C8.31658 14.6834 7.68342 14.6834 7.29289 14.2929L1.70711 8.70711C1.31658 8.31658 1.31658 7.68342 1.70711 7.29289Z"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M8 5V9"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="8" cy="10.5" r="0.5" fill="currentColor" />
    </g>
    <defs>
      <clipPath id="clip0_1037_11752">
        <rect width="16" height="16" fill="white" />
      </clipPath>
    </defs>
  </svg>
);

export default DiamondErrorIcon;
