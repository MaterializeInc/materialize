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
import {
  currentEnvironmentState,
  useEnvironmentGate,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

import {
  CopyableBox,
  SecretCopyableBox,
  TabbedCodeBlock,
} from "./copyableComponents";
import TextLink from "./TextLink";

interface McpConnectInstructionsProps extends BoxProps {
  userStr: string;
  /** Self-managed deployment with OIDC enabled; gets the browser OAuth flow. */
  oidcEnabled?: boolean;
  /** Pre-computed Base64 token for MCP configuration (cloud only). */
  mcpBase64Token?: string;
  /** Callback to generate a new MCP token (creates an app password). */
  onGenerateToken?: () => void;
  /** Whether token generation is in progress. */
  isGeneratingToken?: boolean;
}

const McpConnectInstructions = ({
  userStr,
  oidcEnabled = false,
  mcpBase64Token,
  onGenerateToken,
  isGeneratingToken,
  ...props
}: McpConnectInstructionsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const appConfig = useAppConfig();
  const isCloud = appConfig.mode === "cloud";
  // Envs >= 26.30.0 advertise OAuth via RFC 9728, so MCP clients log in through
  // the browser instead of using a Basic-auth token. Applies to cloud (Frontegg)
  // and self-managed with OIDC. The version must match region-controller's gate
  // for `--frontegg-oauth-issuer-url` (precedence >= 26.30.0). Older/pre-release
  // envs, and self-managed without OIDC, stay on the token flow.
  const oauthAvailable =
    useEnvironmentGate("26.30.0") === true && (isCloud || oidcEnabled);

  const envAddress =
    currentEnvironment?.state === "enabled"
      ? currentEnvironment.httpAddress
      : undefined;

  if (!envAddress) return null;

  // MCP is served on the HTTP endpoint, so build its URL from the HTTP host.
  // `envAddress` is `currentEnvironment.httpAddress`, the same address the
  // console uses for its own HTTP API calls (for self-managed, the console's
  // own host, which nginx proxies to environmentd). The pgwire host advertised
  // in `balancerdDnsNames` does not serve MCP.
  const baseUrl = isCloud
    ? `https://${envAddress.split(":")[0]}`
    : `${appConfig.environmentdScheme}://${envAddress}`;

  const user = userStr || "<user>";
  const base64Command = `printf '${user}:<password>' | base64 -w0`;

  const oauthCommand = (ep: "agent" | "developer") =>
    `claude mcp add --transport http "materialize-${ep}" \\\n  "${baseUrl}/api/mcp/${ep}"`;
  const tokenCommand = (ep: "agent" | "developer") =>
    `claude mcp add --transport http "materialize-${ep}" \\\n  "${baseUrl}/api/mcp/${ep}" \\\n  --header "Authorization: Basic <mcp-token>"`;

  // App-password / token acquisition steps, shared by every endpoint that
  // offers the token method (the Agent tab always, the Developer tab only when
  // OAuth is unavailable).
  const tokenInstructions = (
    <VStack alignItems="stretch" spacing="4">
      {isCloud && onGenerateToken && (
        <VStack alignItems="stretch" spacing="4">
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
            <>
              <Button
                onClick={onGenerateToken}
                variant="primary"
                size="sm"
                alignSelf="flex-start"
              >
                Generate personal MCP token
              </Button>
              <Text fontSize="xs" color={colors.foreground.secondary}>
                For service accounts, create a{" "}
                <TextLink href="/access/app-passwords">
                  service app password
                </TextLink>{" "}
                and Base64-encode it below.
              </Text>
            </>
          )}
        </VStack>
      )}

      <VStack alignItems="stretch" spacing="2">
        {isCloud ? (
          <Text fontSize="sm" color={colors.foreground.secondary}>
            Or Base64-encode an existing app password:
          </Text>
        ) : (
          <Text fontSize="sm" color={colors.foreground.secondary}>
            Base64-encode your username and password:
          </Text>
        )}
        <CopyableBox variant="default" contents={base64Command} />
      </VStack>
    </VStack>
  );

  return (
    <VStack alignItems="stretch" spacing="6" p="6" {...props}>
      <Text fontSize="sm" color={colors.foreground.secondary}>
        Connect your AI agent or coding assistant to Materialize using the
        built-in MCP server.
      </Text>

      <VStack alignItems="stretch" spacing="4">
        <Text textStyle="heading-xs">Connect your client</Text>
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
        <TabbedCodeBlock
          tabs={[
            {
              title: "Agent",
              children: (
                <VStack alignItems="stretch" spacing="4" p="4">
                  <Text fontSize="xs" color={colors.foreground.secondary}>
                    Expose real-time data products to AI agents via
                    Materialize&apos;s built-in MCP endpoint. Used by both
                    service accounts and signed-in users.
                  </Text>

                  {oauthAvailable && (
                    <VStack alignItems="stretch" spacing="2">
                      <Text textStyle="heading-xs">
                        Sign in with your account
                      </Text>
                      <CopyableBox
                        variant="default"
                        wrap
                        contents={oauthCommand("agent")}
                      />
                      <Text fontSize="xs" color={colors.foreground.secondary}>
                        Your browser opens to sign in to Materialize.
                      </Text>
                    </VStack>
                  )}

                  <VStack alignItems="stretch" spacing="4">
                    {oauthAvailable && (
                      <Text textStyle="heading-xs">
                        Or use a service account
                      </Text>
                    )}
                    {tokenInstructions}
                    <CopyableBox
                      variant="default"
                      wrap
                      contents={tokenCommand("agent")}
                    />
                  </VStack>
                </VStack>
              ),
            },
            {
              title: "Developer",
              children: (
                <VStack alignItems="stretch" spacing="4" p="4">
                  <Text fontSize="xs" color={colors.foreground.secondary}>
                    Query Materialize system catalog tables for troubleshooting
                    and observability via the built-in MCP developer endpoint.
                  </Text>

                  {oauthAvailable ? (
                    <VStack alignItems="stretch" spacing="2">
                      <CopyableBox
                        variant="default"
                        wrap
                        contents={oauthCommand("developer")}
                      />
                      <Text fontSize="xs" color={colors.foreground.secondary}>
                        Your browser opens to sign in to Materialize. App
                        passwords also work. See the{" "}
                        <TextLink
                          href={docUrls["/docs/integrations/mcp-server/"]}
                          target="_blank"
                        >
                          documentation
                        </TextLink>
                        .
                      </Text>
                    </VStack>
                  ) : (
                    <VStack alignItems="stretch" spacing="4">
                      {tokenInstructions}
                      <CopyableBox
                        variant="default"
                        wrap
                        contents={tokenCommand("developer")}
                      />
                    </VStack>
                  )}
                </VStack>
              ),
            },
          ]}
        />
      </VStack>
    </VStack>
  );
};

export default McpConnectInstructions;
