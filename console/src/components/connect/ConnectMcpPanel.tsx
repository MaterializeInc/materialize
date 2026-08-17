// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  Flex,
  forwardRef,
  Grid,
  HStack,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  RadioProps,
  Spinner,
  Text,
  useRadio,
  useRadioGroup,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import TextLink from "~/components/TextLink";
import {
  ChevronDownIcon,
  ClaudeLogoIcon,
  CodexLogoIcon,
  CursorLogoIcon,
  OverflowMenuIcon,
  VsCodeLogoIcon,
  WindsurfLogoIcon,
} from "~/icons";
import { useCreateApiToken } from "~/queries/frontegg";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret, toBase64 } from "~/utils/format";

import { ConnectStep, LabeledCommandBox } from "./connectComponents";
import {
  AGENT_SKILLS_COMMAND,
  buildBase64TokenCommand,
  buildMcpInstallLink,
  buildMcpSnippet,
  ConnectContext,
  MCP_CLIENTS,
  MCP_SERVERS,
  MCP_TOKEN_PLACEHOLDER,
  McpClientId,
  McpServerId,
  resolveSignInHint,
} from "./connectOptions";

const CLIENT_ICONS: Record<McpClientId, React.ReactElement> = {
  "claude-code": <ClaudeLogoIcon boxSize="4" />,
  cursor: <CursorLogoIcon boxSize="4" />,
  "claude-desktop": <ClaudeLogoIcon boxSize="4" />,
  vscode: <VsCodeLogoIcon boxSize="4" />,
  windsurf: <WindsurfLogoIcon boxSize="4" />,
  codex: <CodexLogoIcon boxSize="4" />,
  other: <OverflowMenuIcon boxSize="4" />,
};

const McpServerRadioCard = forwardRef<
  RadioProps & { label: string; blurb: string },
  "input"
>(({ label, blurb, ...props }, ref) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { getInputProps, getRadioProps } = useRadio(props);

  return (
    <Box as="label">
      <input ref={ref} {...getInputProps()} />
      <Box
        {...getRadioProps()}
        cursor="pointer"
        px="3"
        py="2.5"
        borderRadius="lg"
        borderWidth="1px"
        borderColor={colors.border.primary}
        bg={colors.background.primary}
        _checked={{
          borderColor: colors.accent.brightPurple,
        }}
        _hover={{
          borderColor: colors.border.secondary,
        }}
        transition="border-color 0.12s ease-out"
      >
        <Text fontSize="sm" fontWeight="500">
          {label}
        </Text>
        <Text fontSize="sm" color={colors.foreground.secondary} mt="0.5">
          {blurb}
        </Text>
      </Box>
    </Box>
  );
});

export interface McpServerSelectProps {
  value: McpServerId;
  onChange: (id: McpServerId) => void;
}

const McpServerSelect = ({ value, onChange }: McpServerSelectProps) => {
  const { getRadioProps, getRootProps } = useRadioGroup({
    name: "mcp-server",
    value,
    onChange: (val) => onChange(val as McpServerId),
  });

  return (
    <Grid {...getRootProps()} templateColumns="1fr 1fr" gap="2.5">
      {MCP_SERVERS.map((server) => (
        <McpServerRadioCard
          key={server.id}
          label={server.label}
          blurb={server.blurb}
          {...getRadioProps({ value: server.id })}
        />
      ))}
    </Grid>
  );
};

export interface McpClientSelectProps {
  value: McpClientId;
  onChange: (id: McpClientId) => void;
}

const McpClientSelect = ({ value, onChange }: McpClientSelectProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const selected =
    MCP_CLIENTS.find((client) => client.id === value) ?? MCP_CLIENTS[0];

  return (
    <Menu matchWidth autoSelect={false}>
      <MenuButton
        type="button"
        maxW="320px"
        px="3"
        py="2"
        borderRadius="lg"
        border="1px solid"
        borderColor={colors.border.secondary}
        bg={colors.background.primary}
        _focusVisible={{ boxShadow: shadows.input.focus }}
        aria-label="MCP client"
      >
        <HStack justifyContent="space-between">
          <HStack spacing="2">
            {CLIENT_ICONS[selected.id]}
            <Text fontSize="sm" fontWeight="500">
              {selected.name}
            </Text>
          </HStack>
          <ChevronDownIcon color={colors.foreground.secondary} />
        </HStack>
      </MenuButton>
      <MenuList>
        {MCP_CLIENTS.map((client) => (
          <MenuItem key={client.id} onClick={() => onChange(client.id)} gap="2">
            {CLIENT_ICONS[client.id]}
            <Text fontSize="sm">{client.name}</Text>
          </MenuItem>
        ))}
      </MenuList>
    </Menu>
  );
};

/** Content of the token acquisition step. Cloud sessions mint an app password
 * that is substituted into the command, self-managed users Base64-encode an
 * existing one. */
const McpTokenStep = ({
  ctx,
  hasToken,
  isGenerating,
  onGenerate,
}: {
  ctx: ConnectContext;
  hasToken: boolean;
  isGenerating: boolean;
  onGenerate: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!ctx.canCreateAppPassword) {
    return (
      <LabeledCommandBox
        label="Base64-encode your username and app password:"
        contents={buildBase64TokenCommand(ctx.user)}
      />
    );
  }

  if (isGenerating) {
    return (
      <Flex alignItems="center" color={colors.foreground.secondary}>
        <Spinner size="sm" mr={2} />
        <Text fontSize="sm">Generating app password...</Text>
      </Flex>
    );
  }

  if (hasToken) {
    return (
      <Text fontSize="sm" color={colors.foreground.secondary}>
        App password created and added to the command below. It will not be
        shown again.
      </Text>
    );
  }

  return (
    <Box>
      <Button onClick={onGenerate} variant="primary" size="sm" px="4">
        Generate app password
      </Button>
    </Box>
  );
};

export interface ConnectMcpPanelProps {
  ctx: ConnectContext;
}

/** MCP Server tab: pick a server, connect a client, install agent skills. */
export const ConnectMcpPanel = ({ ctx }: ConnectMcpPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [serverId, setServerId] = useState<McpServerId>("developer");
  const [clientId, setClientId] = useState<McpClientId>("claude-code");
  const [useTokenFlow, setUseTokenFlow] = useState(false);

  const {
    mutate: createAppPassword,
    isPending: isGeneratingToken,
    data: newPassword,
  } = useCreateApiToken();
  const mcpToken = newPassword?.password
    ? toBase64(`${ctx.user}:${newPassword.password}`)
    : undefined;

  const server =
    MCP_SERVERS.find((candidate) => candidate.id === serverId) ??
    MCP_SERVERS[0];
  const client =
    MCP_CLIENTS.find((candidate) => candidate.id === clientId) ??
    MCP_CLIENTS[0];

  const oauthActive = ctx.oauthAvailable && !useTokenFlow;
  const isDeveloper = server.id === "developer";

  const snippet = buildMcpSnippet({
    client,
    server,
    baseUrl: ctx.mcpBaseUrl,
    token: oauthActive ? undefined : (mcpToken ?? MCP_TOKEN_PLACEHOLDER),
  });
  // Mask the token in the rendered snippet. The copy button copies the real
  // value.
  const displaySnippet = mcpToken
    ? snippet.replace(mcpToken, obfuscateSecret(mcpToken))
    : undefined;
  const installLink =
    oauthActive && client.oneClickInstall
      ? buildMcpInstallLink(client, server, ctx.mcpBaseUrl)
      : undefined;

  const clientStep = (
    <VStack alignItems="stretch" spacing="3">
      <Text fontSize="sm" color={colors.foreground.secondary}>
        Select your client
      </Text>
      <McpClientSelect value={clientId} onChange={setClientId} />
    </VStack>
  );

  const commandStep = (
    <VStack alignItems="stretch" spacing="3">
      {installLink && (
        <HStack spacing="3">
          <Button as="a" href={installLink} variant="primary" size="sm" px="4">
            Add to {client.name}
          </Button>
          <Text fontSize="sm" color={colors.foreground.secondary}>
            Installs the server config for you.
          </Text>
        </HStack>
      )}
      <LabeledCommandBox
        label={installLink ? "Or copy the config:" : undefined}
        contents={snippet}
        displayContents={displaySnippet}
      />
      {oauthActive ? (
        <HStack justifyContent="space-between" alignItems="baseline">
          <Text fontSize="sm" color={colors.foreground.secondary}>
            {resolveSignInHint(client, server.serverName)}
          </Text>
          <TextLink
            as="button"
            type="button"
            fontSize="sm"
            flexShrink={0}
            onClick={() => setUseTokenFlow(true)}
          >
            Use a token instead
          </TextLink>
        </HStack>
      ) : (
        ctx.oauthAvailable && (
          <HStack justifyContent="flex-end">
            <TextLink
              as="button"
              type="button"
              fontSize="sm"
              onClick={() => setUseTokenFlow(false)}
            >
              Use OAuth instead
            </TextLink>
          </HStack>
        )
      )}
    </VStack>
  );

  const skillsStep = (
    <VStack alignItems="stretch" spacing="2">
      <Text fontSize="sm" color={colors.foreground.secondary}>
        Ready-made instructions that help your agent work with Materialize
        accurately.
      </Text>
      <LabeledCommandBox contents={AGENT_SKILLS_COMMAND} />
    </VStack>
  );

  const steps: { title: string; content: React.ReactNode }[] = [
    {
      title: "Choose your MCP server",
      content: <McpServerSelect value={serverId} onChange={setServerId} />,
    },
    { title: "Connect your client", content: clientStep },
  ];
  if (!oauthActive) {
    steps.push({
      title: ctx.canCreateAppPassword
        ? "Generate an app password"
        : "Create a token",
      content: (
        <McpTokenStep
          ctx={ctx}
          hasToken={Boolean(mcpToken)}
          isGenerating={isGeneratingToken}
          onGenerate={() =>
            createAppPassword({ type: "personal", description: "MCP token" })
          }
        />
      ),
    });
  }
  steps.push({
    title:
      !oauthActive && client.tokenConfigLocation
        ? client.tokenConfigLocation
        : client.configLocation,
    content: commandStep,
  });
  if (isDeveloper) {
    steps.push({
      title: "Install agent skills (optional)",
      content: skillsStep,
    });
  }

  return (
    <VStack alignItems="stretch" spacing="0">
      {steps.map((step, index) => (
        <ConnectStep
          key={step.title}
          stepNumber={index + 1}
          title={step.title}
          isLast={index === steps.length - 1}
        >
          {step.content}
        </ConnectStep>
      ))}
    </VStack>
  );
};
