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
  HStack,
  StackProps,
  Text,
  TextProps,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const AsideBox = ({
  title,
  renderCallout,
  ...props
}: React.PropsWithChildren<
  { title: string; renderCallout?: () => React.ReactNode } & StackProps
>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      alignItems="flex-start"
      borderColor={colors.border.primary}
      borderRadius="8px"
      borderWidth="1px"
      spacing="4"
      width="100%"
      pb={renderCallout ? "0" : "2"}
      {...props}
    >
      <Text pt="2" px="4" textStyle="text-ui-med">
        {title}
      </Text>
      <VStack spacing="2" px="4" width="100%" justifyContent="end">
        {props.children}
      </VStack>
      {renderCallout && (
        <VStack
          alignItems="flex-start"
          background={colors.background.secondary}
          p="4"
          width="100%"
          wordBreak="break-all"
        >
          {renderCallout()}
        </VStack>
      )}
    </VStack>
  );
};

type DetailItemProps = {
  label: React.ReactNode;
  rightGutter?: React.ReactNode;
} & TextProps;

const DETAIL_ITEM_SPACING = 4;
const GUTTER_WIDTH = 24 + DETAIL_ITEM_SPACING * 4;
export const DetailItem = ({
  label,
  children,
  rightGutter,
  ...textProps
}: React.PropsWithChildren<DetailItemProps>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack
      width="100%"
      justifyContent="space-between"
      spacing={DETAIL_ITEM_SPACING}
    >
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {label}
      </Text>
      <HStack
        spacing="1"
        justifyContent="end"
        maxW="100%"
        maxWidth={`calc(100% - ${GUTTER_WIDTH}px)`}
      >
        <Text
          textStyle="text-ui-reg"
          noOfLines={1}
          title={typeof children === "string" ? children : undefined}
          {...textProps}
        >
          {children}
        </Text>
        {rightGutter && <Box>{rightGutter}</Box>}
      </HStack>
    </HStack>
  );
};
