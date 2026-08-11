// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { CopyableBox } from "~/components/copyableComponents";
import { MaterializeTheme } from "~/theme";

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
}

/** A copyable command or config snippet with an optional instruction above. */
export const LabeledCommandBox = ({
  contents,
  label,
}: LabeledCommandBoxProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack alignItems="stretch" spacing="1.5">
      {label && (
        <Text fontSize="sm" color={colors.foreground.secondary}>
          {label}
        </Text>
      )}
      <CopyableBox variant="default" wrap contents={contents} />
    </VStack>
  );
};

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
        w="88px"
        flexShrink={0}
      >
        {label}
      </Text>
      <CopyableBox variant="compact" contents={contents} aria-label={label} />
    </HStack>
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
