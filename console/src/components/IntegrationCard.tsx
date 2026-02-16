// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  HStack,
  Image,
  Tag,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useCallback } from "react";
import { Link, useLocation } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import ExternalLinkIcon from "~/svg/ExternalLinkIcon";
import { MaterializeTheme } from "~/theme";

export type IntegrationStatus =
  | "Native"
  | "Partner"
  | "Compatible"
  | "Coming soon";

interface IntegrationCardProps {
  imagePath: string;
  name: string;
  description: string;
  link: string;
  status: IntegrationStatus;
}

const IntegrationCard = ({
  imagePath,
  name,
  description,
  link,
  status,
}: IntegrationCardProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const { track } = useSegment();

  const url = useLocation();
  const isExternalLink = link.startsWith("https://");
  return (
    <Flex
      data-testid="integration-card"
      as={Link}
      to={link}
      onClick={() => {
        track("Integration Card Clicked", { name });
      }}
      target={isExternalLink ? "_blank" : undefined}
      flexDir="column"
      justify="space-between"
      background={colors.components.card.background}
      shadow={shadows.level1}
      transition="all 0.1s"
      _hover={{
        textDecoration: "none",
        shadow: shadows.level2,
        bg: colors.background.secondary,
      }}
      rounded="md"
      minH="240px"
      state={!isExternalLink ? { previousPage: url } : undefined}
    >
      <Flex flexDir="column" gap={6} p={6} pb={4}>
        <HStack w="100%" justify="space-between">
          <Image src={imagePath} h={10} w={10} rounded="md" />
          <StatusTag status={status} />
        </HStack>
        <VStack spacing={2} align="start">
          <Text textStyle="heading-sm">{name}</Text>
          <Text
            textStyle="text-base"
            color={colors.foreground.secondary}
            noOfLines={2}
          >
            {description}
          </Text>
        </VStack>
      </Flex>
      <Flex
        justify="flex-start"
        align="center"
        gap={1}
        px={6}
        pt={4}
        pb={6}
        color={colors.accent.brightPurple}
        data-testid="integration-card-action"
      >
        <Text textStyle="text-ui-med">
          {status === "Coming soon" ? "Get notified" : "View Integration"}
        </Text>{" "}
        {isExternalLink ? (
          <ExternalLinkIcon w={4} h={4} />
        ) : (
          <Text textStyle="text-ui-med">&rarr;</Text>
        )}
      </Flex>
    </Flex>
  );
};

const StatusTag = ({ status }: { status: IntegrationStatus }) => {
  const { colors } = useTheme<MaterializeTheme>();

  const getTagColor = useCallback(
    (s: IntegrationStatus) => {
      switch (s) {
        case "Native":
          return colors.accent.brightPurple;
        case "Partner":
          return colors.background.info;
        case "Compatible":
          return colors.background.tertiary;
        case "Coming soon":
          return colors.background.tertiary;
      }
    },
    [
      colors.background.info,
      colors.background.tertiary,
      colors.accent.brightPurple,
    ],
  );

  const getTextColor = useCallback(
    (s: IntegrationStatus) => {
      switch (s) {
        case "Native":
          return colors.foreground.inverse;
        case "Partner":
          return colors.foreground.primary;
        case "Compatible":
          return colors.foreground.secondary;
        case "Coming soon":
          return colors.foreground.tertiary;
      }
    },
    [
      colors.foreground.primary,
      colors.foreground.secondary,
      colors.foreground.tertiary,
      colors.foreground.inverse,
    ],
  );

  const getHelpText = useCallback((s: IntegrationStatus) => {
    switch (s) {
      case "Native":
        return "This integration is built and maintained by Materialize";
      case "Partner":
        return "This integration is built and maintained by a partner";
      case "Compatible":
        return "Compatible integrations have been tested and verified by Materialize but are not explicitly maintained";
      case "Coming soon":
        return "This integration is not yet available. Contact us to let us know you're interested!";
    }
  }, []);

  return (
    <Tooltip
      label={getHelpText(status as IntegrationStatus)}
      openDelay={500}
      textStyle="text-ui-reg"
      fontWeight="400"
      background={colors.background.inverse}
      rounded="md"
      py={1}
      px={3}
    >
      <Tag
        size="sm"
        background={getTagColor(status as IntegrationStatus)}
        color={getTextColor(status as IntegrationStatus)}
        variant="solid"
        textStyle="text-ui-med"
        rounded="full"
      >
        {status}
      </Tag>
    </Tooltip>
  );
};

export default IntegrationCard;
