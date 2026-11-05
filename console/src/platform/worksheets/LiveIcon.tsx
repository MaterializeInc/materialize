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

/** A lightning bolt, for live (SUBSCRIBE) results. */
export const LiveIcon = createIcon({
  displayName: "LiveIcon",
  viewBox: "0 0 16 16",
  path: (
    <path
      d="M8.8 1.5 3 9h4.4l-.9 5.5L12.5 7H8.1l.7-5.5Z"
      fill="currentColor"
      stroke="currentColor"
      strokeWidth="1"
      strokeLinejoin="round"
    />
  ),
});
