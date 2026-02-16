// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * List view of deployments or other materialize primitives.
 */

import { Box, BoxProps, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import Missing from "~/svg/Missing";
import NoIcon from "~/svg/NoIcon";
import { MaterializeTheme } from "~/theme";

/*
 * Composable components for the empty list view
 */

type EmptyType = "Empty" | "Missing" | "Error";

export const EmptyListWrapper = (props: BoxProps) => (
  <VStack
    alignItems="center"
    justifyContent="center"
    textAlign="center"
    flex={1}
    spacing={8}
    h="100%"
    w="100%"
    {...props}
  >
    {props.children}
  </VStack>
);

export const EmptyListHeader = (props: BoxProps) => (
  <VStack
    alignItems="center"
    justifyContent="center"
    spacing={6}
    maxW="460px"
    textAlign="center"
  >
    {props.children}
  </VStack>
);

type IconBoxProps = BoxProps & {
  type?: EmptyType;
};

export const IconBox = ({ type, children }: IconBoxProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  let overlapIcon = (
    <NoIcon
      fillColor={colors.foreground.primary}
      bgColor={colors.background.primary}
    />
  );
  switch (type) {
    case "Missing":
      overlapIcon = (
        <Missing
          fillColor={colors.foreground.primary}
          bgColor={colors.background.primary}
        />
      );
      break;
  }
  return (
    <Box color={colors.border.primary} h="10" w="10" position="relative">
      <Box
        p="8px"
        position="absolute"
        top="2px"
        left="center"
        h="40px"
        w="40px"
      >
        {overlapIcon}
      </Box>
      {children}
    </Box>
  );
};

type EmptyListHeaderContentsProps = {
  title: string;
  helpText?: React.ReactNode;
};

export const EmptyListHeaderContents = ({
  title,
  helpText,
}: EmptyListHeaderContentsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing={2}>
      <Text as="h2" textStyle="heading-sm" color={colors.foreground.primary}>
        {title}
      </Text>
      {helpText && (
        <Text as="div" color={colors.foreground.secondary}>
          {helpText}
        </Text>
      )}
    </VStack>
  );
};

type SampleCodeBoxWrapperProps = BoxProps & {
  docsUrl?: string;
};

export const SampleCodeBoxWrapper = (props: SampleCodeBoxWrapperProps) => {
  return (
    <VStack
      alignItems="center"
      justifyContent="center"
      spacing={4}
      width="460px"
    >
      {props.children}
      <Text fontSize="sm" textAlign="left" width="full">
        Having trouble?{" "}
        <TextLink href={props.docsUrl || docUrls["/docs/"]} target="_blank">
          View the documentation.
        </TextLink>
      </Text>
    </VStack>
  );
};
