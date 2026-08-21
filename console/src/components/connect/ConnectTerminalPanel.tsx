// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { LabeledCommandBox } from "./connectComponents";
import { buildPsqlCommand, ConnectContext } from "./connectOptions";

export interface ConnectTerminalPanelProps {
  ctx: ConnectContext;
}

/** Terminal tab: the psql one-liner. */
export const ConnectTerminalPanel = ({ ctx }: ConnectTerminalPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const command = buildPsqlCommand(
    {
      host: ctx.host,
      port: ctx.port,
      database: ctx.database,
      user: ctx.user,
      ssl: ctx.ssl,
    },
    ctx.clusterName,
  );

  return (
    <VStack alignItems="stretch" spacing="3">
      <LabeledCommandBox
        label="Run this in your terminal:"
        contents={command}
      />
      {ctx.idToken && (
        <Text fontSize="sm" color={colors.foreground.secondary}>
          When prompted for a password, paste the ID token from the External
          tools tab.
        </Text>
      )}
    </VStack>
  );
};
