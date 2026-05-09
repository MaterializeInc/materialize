// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  BoxProps,
  Button,
  Flex,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";

import { useAppConfig } from "~/config/useAppConfig";
import docUrls from "~/mz-doc-urls.json";
import { currentEnvironmentState } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

import { CopyableBox, SecretCopyableBox } from "./copyableComponents";
import TextLink from "./TextLink";

interface McpConnectInstructionsProps extends BoxProps {
  userStr: string;
  /** Pre-computed Base64 token for MCP configuration (cloud only). */
  mcpBase64Token?: string;
  /** Callback to generate a new MCP token (creates an app password). */
  onGenerateToken?: () => void;
  /** Whether token generation is in progress. */
  isGeneratingToken?: boolean;
}

const McpConnectInstructions = ({
  userStr,
  mcpBase64Token,
  onGenerateToken,
  isGeneratingToken,
  ...props
}: McpConnectInstructionsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const appConfig = useAppConfig();
  const isCloud = appConfig.mode === "cloud";
  const endpoint = "developer";

  const envAddress =
    currentEnvironment?.state === "enabled"
      ? currentEnvironment.httpAddress
      : undefined;

  if (!envAddress) return null;

  // Cloud: HTTPS with the environment's HTTP address hostname.
  // Self-managed: Use a placeholder since the MCP endpoint may be behind a
  // load balancer or custom domain that we can't determine from the console.
  const baseUrl = isCloud
    ? `https://${envAddress.split(":")[0]}`
    : "<your-materialize-host>";

  const user = userStr || "<user>";
  const base64Command = `printf '${user}:<password>' | base64 -w0`;

  const endpointUrl = `${baseUrl}/api/mcp/${endpoint}`;
  const claudeCodeCliCommand = `claude mcp add --transport http materialize-${endpoint} \\\n  ${endpointUrl} \\\n  --header "Authorization: Basic <mcp-token>"`;

  return (
    <VStack
      alignItems="stretch"
      spacing="4"
      p="6"
      overflowY="auto"
      maxHeight="60vh"
      {...props}
    >
      <Text fontSize="sm" color={colors.foreground.secondary}>
        Connect your AI agent or coding assistant to Materialize using the
        built-in MCP server.
      </Text>

    <VStack
      alignItems="stretch"
      spacing="6"
      p="6"
      overflowY="auto"
      maxHeight="60vh"
      {...props}
    >
        <Text textStyle="heading-xs">1. Get your MCP token</Text>

        {isCloud && onGenerateToken && (
          <VStack alignItems="stretch" spacing="2">
            <Text fontSize="sm" color={colors.foreground.secondary}>
              Generate a new token:
            </Text>
            {isGeneratingToken ? (
              <Flex alignItems="center" color={colors.foreground.secondary}>
                <Spinner size="sm" mr={2} />
                <Text fontSize="sm">Generating token...</Text>
              </Flex>
            ) : mcpBase64Token ? (
              <VStack alignItems="stretch" spacing="1">
                <SecretCopyableBox
                  label="mcpToken"
                  contents={mcpBase64Token}
                  obfuscatedContent={obfuscateSecret(mcpBase64Token)}
                  overflow="hidden"
                  minWidth={0}
                />
                <Text
                  fontSize="xs"
                  color={colors.foreground.secondary}
                  lineHeight="tall"
                >
                  Copy this somewhere safe. Tokens cannot be displayed after
                  initial creation.
                </Text>
              </VStack>
            ) : (
              <Button
                onClick={onGenerateToken}
                variant="primary"
                size="sm"
                alignSelf="flex-start"
              >
                Generate MCP token
              </Button>
            )}
          </VStack>
        )}

        <VStack alignItems="stretch" spacing="2">
          {isCloud && (
            <Text fontSize="sm" color={colors.foreground.secondary}>
              Or Base64-encode an existing app password:
            </Text>
          )}
          {!isCloud && (
            <Text fontSize="sm" color={colors.foreground.secondary}>
              Base64-encode your username and password:
            </Text>
          )}
          <CopyableBox variant="default" contents={base64Command} />
        </VStack>
      </VStack>

      <VStack alignItems="stretch" spacing="3">
        <Text textStyle="heading-xs">2. Connect your client</Text>
        <Text fontSize="sm" color={colors.foreground.secondary}>
          See the{" "}
          <TextLink
            href={docUrls["/docs/integrations/mcp-server/"]}
            target="_blank"
          >
            documentation
          </TextLink>{" "}
          for connecting clients like Claude Desktop, Cursor, Windsurf, etc.
        </Text>
        <Text fontSize="sm" color={colors.foreground.secondary}>
          Or connect to Claude Code now:
        </Text>
        <CopyableBox variant="default" wrap contents={claudeCodeCliCommand} />
      </VStack>
    </VStack>
  );
};

export default McpConnectInstructions;
