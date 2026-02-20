// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  Collapse,
  HStack,
  IconButton,
  Link,
  LinkProps,
  UnorderedList,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import { useSegment } from "~/analytics/segment";
import ChevronDownIcon from "~/svg/ChevronDownIcon";
import ExternalLinkIcon from "~/svg/ExternalLinkIcon";
import ThumbIcon from "~/svg/ThumbIcon";
import { MaterializeTheme } from "~/theme";

export const NoticeContainer = ({
  children,
}: {
  children?: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      borderWidth="1px"
      borderColor={colors.border.warn}
      backgroundColor={colors.background.warn}
      borderRadius="lg"
      width="100%"
      alignItems="flex-start"
    >
      {children}
    </VStack>
  );
};

export const NoticeContent = ({ children }: { children?: React.ReactNode }) => {
  return (
    <VStack py="2" px="4" alignItems="flex-start" spacing="0" width="100%">
      {children}
    </VStack>
  );
};

export const NoticeExternalLink = ({
  children,
  ...rest
}: LinkProps & {
  insightVersionedId: string;
  redactedSql?: string;
}) => {
  const { track } = useSegment();
  return (
    <Link
      textStyle="text-ui-med"
      onClick={() => {
        track("Insights External Link Clicked", {
          href: rest.href,
          insightVersionedId: rest.insightVersionedId,
          redactedSql: rest.redactedSql,
        });
      }}
      isExternal
      {...rest}
    >
      {children}
      <ExternalLinkIcon mx="2" mb="1" />
    </Link>
  );
};

const FEEDBACK_BUTTON_HOVER_STYLE = {
  filter: "brightness(0.95)",
};

const FEEDBACK_BUTTON_ACTIVE_STYLE = {
  filter: "brightness(0.90)",
};

export const NoticeFooter = ({
  children,
  onThumbsUp,
  onThumbsDown,
  insightVersionedId,
  redactedSql,
}: {
  children?: React.ReactNode;
  onThumbsUp?: () => void;
  onThumbsDown?: () => void;
  insightVersionedId: string;
  redactedSql?: string;
}) => {
  const { track } = useSegment();

  const { colors } = useTheme<MaterializeTheme>();

  const [activeThumb, setActiveThumb] = useState<"up" | "down" | null>(null);

  const trackIsUseful = (isUseful: boolean) => {
    track("Insights Thumbs Feedback", {
      insightVersionedId,
      redactedSql,
      isUseful,
    });
  };

  return (
    <HStack
      width="100%"
      borderTopWidth="1px"
      borderColor={colors.border.warn}
      py="1"
      px="4"
    >
      <HStack flexGrow="0" flexWrap="wrap">
        {children}
      </HStack>
      <HStack spacing="2" flexGrow="1" flexShrink="0" justifyContent="flex-end">
        <IconButton
          size="sm"
          aria-label="Thumbs up"
          {...(activeThumb === "up" ? FEEDBACK_BUTTON_ACTIVE_STYLE : {})}
          icon={
            <ThumbIcon
              color={
                activeThumb === "up" ? colors.accent.brightPurple : undefined
              }
            />
          }
          variant="unstyled"
          onClick={() => {
            trackIsUseful(true);
            setActiveThumb("up");
            onThumbsUp?.();
          }}
          backgroundColor={colors.background.warn}
          _hover={FEEDBACK_BUTTON_HOVER_STYLE}
          _active={FEEDBACK_BUTTON_ACTIVE_STYLE}
        />
        <IconButton
          aria-label="Thumbs down"
          size="sm"
          variant="unstyled"
          {...(activeThumb === "down" ? FEEDBACK_BUTTON_ACTIVE_STYLE : {})}
          icon={
            <ThumbIcon
              transform="rotate(180deg)"
              color={
                activeThumb === "down" ? colors.accent.brightPurple : undefined
              }
            />
          }
          onClick={() => {
            trackIsUseful(false);
            setActiveThumb("down");
            onThumbsDown?.();
          }}
          backgroundColor={colors.background.warn}
          _hover={FEEDBACK_BUTTON_HOVER_STYLE}
          _active={FEEDBACK_BUTTON_ACTIVE_STYLE}
        />
      </HStack>
    </HStack>
  );
};

export const NoticeUnorderedList = ({
  list,
}: {
  list: Array<React.ReactNode>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { isOpen: showLess, onToggle: onToggleShowMore } = useDisclosure();

  const firstThreeListItems = list.slice(0, 3);

  const remainingListItems = list.slice(3);

  return (
    <VStack alignItems="flex-start" spacing="1" width="100%" overflow="auto">
      <UnorderedList marginStart="1rem">
        {firstThreeListItems.map((item) => item)}
        <Collapse in={remainingListItems.length > 0 && showLess}>
          {remainingListItems.map((item) => item)}
        </Collapse>
      </UnorderedList>
      {remainingListItems.length > 0 && (
        <Button
          variant="text-only"
          size="xs"
          color={colors.accent.brightPurple}
          onClick={onToggleShowMore}
          rightIcon={
            showLess ? (
              <ChevronDownIcon
                transform="rotate(180deg)"
                color="currentcolor"
                ml="-1"
              />
            ) : (
              <ChevronDownIcon color="currentcolor" ml="-1" />
            )
          }
        >
          {showLess ? "Show less" : "Show more"}
        </Button>
      )}
    </VStack>
  );
};
