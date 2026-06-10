// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, CloseButton, HStack, Text, useTheme } from "@chakra-ui/react";
import { UseToastOptions as UseChakraToastOptions } from "@chakra-ui/toast";
import React from "react";

import { CheckmarkIconWithCircle } from "~/svg/CheckmarkIcon";
import WarningIcon from "~/svg/WarningIcon";
import { MaterializeTheme } from "~/theme";
export interface UseToastOptions {
  status?: "success" | "error";
  duration?: UseChakraToastOptions["duration"];
  position?: UseChakraToastOptions["position"];
  render?: UseChakraToastOptions["render"];
  description?: UseChakraToastOptions["description"];
  icon?: UseChakraToastOptions["icon"];
  isClosable: UseChakraToastOptions["isClosable"];
}
export interface ToastComponentProps extends UseToastOptions {
  onClose(): void;
}

export const ObjectToastDescription = ({
  name,
  message,
}: {
  name: string;
  message: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <>
      {name}{" "}
      <Text color={colors.foreground.secondary} as="span">
        {message}
      </Text>
    </>
  );
};

export const ToastComponent = ({
  icon: iconOverride,
  description,
  status,
  onClose,
  isClosable,
}: ToastComponentProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  let icon = null;

  if (iconOverride) {
    icon = iconOverride;
  } else if (status === "success") {
    icon = <CheckmarkIconWithCircle />;
  } else if (status === "error") {
    icon = <WarningIcon height="6" width="6" color={colors.accent.red} />;
  }

  return (
    <Box
      bg={colors.background.primary}
      alignItems="start"
      shadow={shadows.level3}
      border="1px solid"
      borderRadius="lg"
      borderColor={colors.border.primary}
      px="6"
      py="4"
      width="auto"
      minW="360px"
      m="6"
      position="relative"
    >
      <HStack spacing="4" alignItems="flex-start">
        {icon && <Box>{icon}</Box>}
        <Text as="div" fontWeight="500" fontSize="sm">
          {description}
        </Text>
      </HStack>
      {isClosable && (
        <CloseButton
          size="sm"
          onClick={onClose}
          position="absolute"
          insetEnd={1}
          top={1}
        />
      )}
    </Box>
  );
};
