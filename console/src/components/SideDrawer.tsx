// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import {
  Box,
  Drawer as ChakraDrawer,
  DrawerBody,
  DrawerContent,
  DrawerHeader,
  DrawerOverlay,
  DrawerProps,
  Flex,
  IconButton,
  useBreakpointValue,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export interface SideDrawerProps extends Omit<DrawerProps, "placement"> {
  title?: React.ReactNode;
  showCloseButton?: boolean;
  headerActions?: React.ReactNode;
  children: React.ReactNode;
  width?: string;
}

export const SideDrawer = ({
  title,
  showCloseButton = true,
  headerActions,
  children,
  size = "md",
  width,
  ...drawerProps
}: SideDrawerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const blockScrollOnMount = useBreakpointValue(
    {
      base: false,
      lg: true,
    },
    { ssr: false },
  );

  return (
    <ChakraDrawer
      placement="right"
      blockScrollOnMount={blockScrollOnMount}
      size={size}
      {...drawerProps}
    >
      <DrawerOverlay />
      <DrawerContent maxW={width}>
        {(title || showCloseButton || headerActions) && (
          <DrawerHeader
            display="flex"
            alignItems="center"
            justifyContent="space-between"
            borderBottom="1px solid"
            borderColor={colors.border.primary}
            py={3}
            px={4}
          >
            <Flex alignItems="center" gap={2} flex={1} minWidth={0}>
              {title && (
                <Box
                  textStyle="heading-sm"
                  overflow="hidden"
                  textOverflow="ellipsis"
                  whiteSpace="nowrap"
                >
                  {title}
                </Box>
              )}
            </Flex>
            <Flex alignItems="center" gap={2}>
              {headerActions}
              {showCloseButton && (
                <IconButton
                  aria-label="Close drawer"
                  icon={<CloseIcon boxSize={3} />}
                  variant="ghost"
                  size="sm"
                  onClick={drawerProps.onClose}
                />
              )}
            </Flex>
          </DrawerHeader>
        )}
        <DrawerBody p={0} overflow="auto">
          {children}
        </DrawerBody>
      </DrawerContent>
    </ChakraDrawer>
  );
};

export default SideDrawer;
