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
  HStack,
  Input,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import {
  CopyableBox,
  SecretCopyableBox,
} from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import { useCreateApiToken } from "~/queries/frontegg";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

export interface ConnectStepProps {
  stepNumber: number;
  title: string;
  isLast?: boolean;
  children: React.ReactNode;
}

/** Numbered step with a connector line to the next step. */
export const ConnectStep = ({
  stepNumber,
  title,
  isLast = false,
  children,
}: ConnectStepProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack alignItems="stretch" spacing="4">
      <VStack spacing="0">
        <Flex
          boxSize="6"
          flexShrink={0}
          borderRadius="base"
          border="1px solid"
          borderColor={colors.border.secondary}
          bg={colors.background.primary}
          alignItems="center"
          justifyContent="center"
          fontSize="xs"
          fontWeight="500"
          color={colors.foreground.secondary}
        >
          {stepNumber}
        </Flex>
        {!isLast && (
          <Box flex="1" w="1px" bg={colors.border.primary} mt="1.5" mb="-1" />
        )}
      </VStack>
      <Box flex="1" pb={isLast ? 0 : 7} minW={0}>
        <Text fontSize="sm" fontWeight="500" mb="2" mt="0.5">
          {title}
        </Text>
        {children}
      </Box>
    </HStack>
  );
};

export interface LabeledCommandBoxProps {
  contents: string;
  label?: React.ReactNode;
  obfuscatedContents?: string;
}

/** A copyable command or config snippet with an optional instruction above. */
export const LabeledCommandBox = ({
  contents,
  label,
  obfuscatedContents,
}: LabeledCommandBoxProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack alignItems="stretch" spacing="1.5">
      {label && (
        <Text fontSize="sm" color={colors.foreground.secondary}>
          {label}
        </Text>
      )}
      <CopyableBox
        variant="default"
        wrap
        contents={contents}
        obfuscatedContents={obfuscatedContents}
      />
    </VStack>
  );
};

const DETAIL_LABEL_WIDTH = "88px";

export interface ConnectionDetailRowProps {
  label: string;
  contents: string;
}

/** Label and copyable value row for connection details. */
export const ConnectionDetailRow = ({
  label,
  contents,
}: ConnectionDetailRowProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack alignItems="center" spacing="3">
      <Text
        fontSize="sm"
        color={colors.foreground.secondary}
        w={DETAIL_LABEL_WIDTH}
        flexShrink={0}
      >
        {label}
      </Text>
      <CopyableBox
        variant="compact"
        contents={contents}
        aria-label={label}
        flex="1"
        minW={0}
        w="auto"
      />
    </HStack>
  );
};

export interface CreateAppPasswordRowProps {
  onCreated?: (password: string) => void;
}

/** Inline app password creation: name it, create it, copy it once. */
export const CreateAppPasswordRow = ({
  onCreated,
}: CreateAppPasswordRowProps) => {
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
      onCreated?.(newPassword.password);
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

export interface IdTokenRowProps {
  idToken: string;
}

/** Self-managed OIDC deployments use the ID token as the SQL password. */
export const IdTokenRow = ({ idToken }: IdTokenRowProps) => {
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

export interface ConnectMethodCardProps {
  isActive: boolean;
  label: string;
  sublabel: string;
  icon: React.ReactNode;
  onClick: () => void;
}

/** Top-level card for switching between connection methods. */
export const ConnectMethodCard = ({
  isActive,
  label,
  sublabel,
  icon,
  onClick,
}: ConnectMethodCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      as="button"
      type="button"
      onClick={onClick}
      spacing="1"
      py="3"
      px="2"
      borderRadius="lg"
      borderWidth="1px"
      borderColor={isActive ? colors.border.secondary : colors.border.primary}
      bg={isActive ? colors.background.secondary : colors.background.primary}
      _hover={{ bg: colors.background.secondary }}
      transition="all 0.12s ease-out"
      aria-pressed={isActive}
    >
      <Box
        mb="0.5"
        color={
          isActive ? colors.foreground.primary : colors.foreground.secondary
        }
      >
        {icon}
      </Box>
      <Text fontSize="sm" fontWeight="500" lineHeight="1.3">
        {label}
      </Text>
      <Text fontSize="xs" lineHeight="1.3" color={colors.foreground.secondary}>
        {sublabel}
      </Text>
    </VStack>
  );
};
