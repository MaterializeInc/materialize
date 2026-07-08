// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text } from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";

export interface UsagePrivilegeAlertProps {
  action: string;
}

export const UsagePrivilegeAlert = ({ action }: UsagePrivilegeAlertProps) => (
  <Alert
    variant="info"
    message={
      <Text>
        You&apos;ll need{" "}
        <Text as="span" textStyle="monospace">
          USAGE
        </Text>{" "}
        privilege on this cluster to {action}.
      </Text>
    }
  />
);
