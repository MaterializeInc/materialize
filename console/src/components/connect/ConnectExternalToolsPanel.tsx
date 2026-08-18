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
  HStack,
  Input,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import { SecretCopyableBox } from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import { useCreateApiToken } from "~/queries/frontegg";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

import {
  ConnectionDetailRow,
  ConnectStep,
  LabeledCommandBox,
} from "./connectComponents";
import {
  ConnectContext,
  EXTERNAL_TOOLS,
  ExternalToolId,
} from "./connectOptions";

const DETAIL_LABEL_WIDTH = "88px";

/** Inline app password creation: name it, create it, copy it once. */
const CreateAppPasswordRow = ({
  onCreated,
}: {
  onCreated: (password: string) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [isNaming, setIsNaming] = useState(false);
  const [name, setName] = useState("");
  const {
    mutate: createAppPassword,
    isPending,
    data: newPassword,
  } = useCreateApiToken();

  React.useEffect(() => {
    if (newPassword?.password) {
      onCreated(newPassword.password);
    }
  }, [newPassword, onCreated]);

  const create = () => {
    if (name.trim().length === 0 || isPending) return;
    createAppPassword({ type: "personal", description: name.trim() });
  };

  return (
    <Box>
      <HStack alignItems="center" spacing="3">
        <Text
          fontSize="sm"
          color={colors.foreground.secondary}
          w={DETAIL_LABEL_WIDTH}
          flexShrink={0}
        >
          Password
        </Text>
        {newPassword?.password ? (
          <SecretCopyableBox
            label="Password"
            contents={newPassword.password}
            obfuscatedContent={newPassword.obfuscatedPassword}
            overflow="hidden"
            flex="1"
            w="auto"
            minWidth={0}
          />
        ) : isNaming ? (
          <HStack spacing="2" flex="1">
            <Input
              value={name}
              onChange={(event) => setName(event.target.value)}
              maxW="220px"
              isDisabled={isPending}
              placeholder="Password name"
              aria-label="App password name"
              autoFocus
              onKeyDown={(event) => {
                if (event.key === "Enter") create();
              }}
            />
            <Button
              variant="primary"
              size="sm"
              px="3"
              flexShrink={0}
              isLoading={isPending}
              loadingText="Creating"
              isDisabled={name.trim().length === 0}
              onClick={create}
            >
              Create
            </Button>
            <Button
              variant="borderless"
              size="sm"
              flexShrink={0}
              isDisabled={isPending}
              onClick={() => {
                setIsNaming(false);
                setName("");
              }}
            >
              Cancel
            </Button>
          </HStack>
        ) : (
          <Button
            variant="secondary"
            size="sm"
            onClick={() => setIsNaming(true)}
          >
            Create new password
          </Button>
        )}
      </HStack>
      {isNaming && !newPassword?.password && (
        <Text
          fontSize="sm"
          color={colors.foreground.secondary}
          mt="1.5"
          ml={`calc(${DETAIL_LABEL_WIDTH} + 12px)`}
        >
          You are naming this password. The password itself is generated for
          you.
        </Text>
      )}
      {newPassword?.password && (
        <Text
          fontSize="sm"
          color={colors.foreground.secondary}
          mt="1.5"
          ml={`calc(${DETAIL_LABEL_WIDTH} + 12px)`}
        >
          Copy it now. It will not be shown again. Manage in{" "}
          <TextLink href="/access/app-passwords">App Passwords</TextLink>.
        </Text>
      )}
    </Box>
  );
};

/** Self-managed OIDC deployments use the ID token as the SQL password. */
const IdTokenRow = ({ idToken }: { idToken: string }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box>
      <HStack alignItems="center" spacing="3">
        <Text
          fontSize="sm"
          color={colors.foreground.secondary}
          w={DETAIL_LABEL_WIDTH}
          flexShrink={0}
        >
          Password
        </Text>
        <SecretCopyableBox
          label="idToken"
          contents={idToken}
          obfuscatedContent={obfuscateSecret(idToken)}
          overflow="hidden"
          flex="1"
          w="auto"
          minWidth={0}
        />
      </HStack>
      <Text
        fontSize="sm"
        color={colors.foreground.secondary}
        mt="1.5"
        ml={`calc(${DETAIL_LABEL_WIDTH} + 12px)`}
      >
        When prompted for a password, paste this ID token.
      </Text>
    </Box>
  );
};

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
  // Mask the password in the rendered snippet. The copy button copies the
  // real value.
  const displaySnippet = createdPassword
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
            displayContents={displaySnippet}
          />
        </VStack>
      </ConnectStep>
    </VStack>
  );
};
