// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createIcon } from "@chakra-ui/react";
import React from "react";

/** A push pin, for results kept in the pane. */
export const PinIcon = createIcon({
  displayName: "PinIcon",
  viewBox: "0 0 16 16",
  path: (
    <g
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M9.5 1.75 14.25 6.5l-1.5.5-2.5 2.5-.5 3.75-6.75-6.75 3.75-.5 2.5-2.5Z" />
      <path d="M5.75 10.25 1.75 14.25" />
    </g>
  ),
});
