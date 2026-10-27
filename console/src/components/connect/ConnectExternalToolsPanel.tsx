// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, useTheme, VStack } from "@chakra-ui/react";
import React, { useState } from "react";

import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

import {
  ConnectionDetailRow,
  ConnectStep,
  CreateAppPasswordRow,
  IdTokenRow,
  LabeledCommandBox,
} from "./connectComponents";
import {
  ConnectContext,
  EXTERNAL_TOOLS,
  ExternalToolId,
} from "./connectOptions";

export interface ConnectExternalToolsPanelProps {
  ctx: ConnectContext;
}

/** External tools tab: connection details plus per-tool config snippets. */
export const ConnectExternalToolsPanel = ({
  ctx,
}: ConnectExternalToolsPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [toolId, setToolId] = useState<ExternalToolId>("dbeaver");
  const [createdPassword, setCreatedPassword] = useState<string>();

  const tool =
    EXTERNAL_TOOLS.find((candidate) => candidate.id === toolId) ??
    EXTERNAL_TOOLS[0];
  const snippet = tool.buildSnippet({
    host: ctx.host,
    port: ctx.port,
    database: ctx.database,
    user: ctx.user,
    password: createdPassword,
    ssl: ctx.ssl,
  });
  const obfuscatedSnippet = createdPassword
    ? snippet.replaceAll(createdPassword, obfuscateSecret(createdPassword))
    : undefined;

  return (
    <VStack alignItems="stretch" spacing="0">
      <ConnectStep stepNumber={1} title="Get your connection details">
        <VStack alignItems="stretch" spacing="2">
          <ConnectionDetailRow label="Host" contents={ctx.host} />
          <ConnectionDetailRow label="Port" contents={ctx.port} />
          <ConnectionDetailRow label="Database" contents={ctx.database} />
          <ConnectionDetailRow label="User" contents={ctx.user} />
          {ctx.canCreateAppPassword && (
            <CreateAppPasswordRow onCreated={setCreatedPassword} />
          )}
          {ctx.idToken && <IdTokenRow idToken={ctx.idToken} />}
        </VStack>
      </ConnectStep>
      <ConnectStep stepNumber={2} title="Connect your tool" isLast>
        <VStack alignItems="stretch" spacing="3">
          <HStack spacing="1">
            {EXTERNAL_TOOLS.map((candidate) => {
              const isActive = candidate.id === toolId;
              return (
                <Box
                  key={candidate.id}
                  as="button"
                  type="button"
                  aria-pressed={isActive}
                  px="3"
                  py="1.5"
                  borderRadius="lg"
                  fontSize="sm"
                  fontWeight="500"
                  bg={isActive ? colors.background.secondary : "transparent"}
                  color={
                    isActive
                      ? colors.foreground.primary
                      : colors.foreground.secondary
                  }
                  border="1px solid"
                  borderColor={
                    isActive ? colors.border.secondary : "transparent"
                  }
                  _hover={{ bg: colors.background.secondary }}
                  transition="all 0.1s ease-out"
                  onClick={() => setToolId(candidate.id)}
                >
                  {candidate.label}
                </Box>
              );
            })}
          </HStack>
          <LabeledCommandBox
            label={tool.instruction}
            contents={snippet}
            obfuscatedContents={obfuscatedSnippet}
          />
        </VStack>
      </ConnectStep>
    </VStack>
  );
};
