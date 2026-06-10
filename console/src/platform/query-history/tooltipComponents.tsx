// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, IconProps, LinkProps, Tooltip } from "@chakra-ui/react";
import React from "react";

import { InfoIcon } from "~/icons";
import docUrls from "~/mz-doc-urls.json";

export const TransactionIsolationLevelTooltip = (props: LinkProps) => {
  return (
    <Tooltip
      label="The chosen isolation level determines consistency guarantees and may affect latency. Click to learn more."
      placement="end"
    >
      <chakra.a
        href={docUrls["/docs/get-started/isolation-level/"]}
        target="_blank"
        {...props}
      >
        <InfoIcon />
      </chakra.a>
    </Tooltip>
  );
};

export const ThrottledCountTooltip = (props: IconProps) => {
  return (
    <Tooltip label="The number of statements that were not recorded before the current one due to throttling.">
      <InfoIcon {...props} />
    </Tooltip>
  );
};
