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
  Grid,
  HStack,
  StackDivider,
  Tag,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { ErrorBoundary } from "@sentry/react";
import { useAtomValue } from "jotai";
import React, { Suspense } from "react";

import {
  useCurrentOrganization,
  useMaybeCurrentOrganizationId,
} from "~/api/auth";
import { isIncident } from "~/api/incident-io/types";
import { Alert } from "~/components/Alert";
import { Card, CardContent, CardTitle } from "~/components/cardComponents";
import { CopyButton } from "~/components/copyableComponents";
import { isInRegion } from "~/components/IncidentStatusWidget/utils";
import TextLink from "~/components/TextLink";
import {
  COMMUNITY_SLACK_URL,
  HELP_CENTER_TICKETS_URL,
  HELP_CENTER_URL,
  STATUS_PAGE_URL,
  SUPPORT_EMAIL,
  SUPPORT_PLANS_URL,
} from "~/externalUrls";
import {
  openIntercomMessenger,
  openIntercomNewConversation,
  useHasIntercomJwt,
  useIntercomUnreadCount,
  useIsIntercomIdentified,
} from "~/hooks/useIntercom";
import { InfoIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import docUrls from "~/mz-doc-urls.json";
import { useSummary } from "~/queries/incident-io";
import { currentRegionIdAtom } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

export const SUPPORT_MAILTO = `mailto:${SUPPORT_EMAIL}`;
export const OUTAGE_MAILTO = `${SUPPORT_MAILTO}?subject=${encodeURIComponent(
  "Urgent: production outage",
)}`;

const EXTERNAL_LINK_PROPS = {
  target: "_blank",
  rel: "noopener noreferrer",
} as const;

/**
 * Opens the Intercom Messenger instead of following the link when the
 * Messenger is booted for the signed-in user. Otherwise the link's href (email
 * or the Help Center) is the fallback. `open` checks that the Messenger has
 * booted at click time, so a late boot is picked up without waiting for the
 * next readiness check.
 */
const preferIntercom =
  (hasJwt: boolean, open: () => boolean) =>
  (event: React.MouseEvent<HTMLElement>) => {
    if (hasJwt && open()) event.preventDefault();
  };

const SEVERITIES = [
  {
    level: "Urgent",
    description: "Production down, no workaround. Use the outage button.",
    isUrgent: true,
  },
  { level: "High", description: "Key features seriously impaired" },
  { level: "Normal", description: "Some impact, a workaround exists" },
  { level: "Low", description: "Little or no impact, or a question" },
];

const statusPageButtonProps = (href: string) => ({
  as: "a" as const,
  href,
  ...EXTERNAL_LINK_PROPS,
});

/** Shown instead of live status while it loads, or if it can't be fetched. */
const StatusLink = () => (
  <TextLink
    data-testid="support-status-link"
    href={STATUS_PAGE_URL}
    fontWeight="500"
    {...EXTERNAL_LINK_PROPS}
  >
    Check system status ↗
  </TextLink>
);

const SupportStatusBannerInner = () => {
  const { data: summary, isError } = useSummary();
  const regionId = useAtomValue(currentRegionIdAtom);

  // A failed refresh keeps the last good data cached, and suspense queries
  // only throw to the error boundary when there's no data. Don't present a
  // stale "operational" (or a stale incident) as current.
  if (isError) {
    return <StatusLink />;
  }

  const event =
    summary.ongoing_incidents[0] ??
    summary.in_progress_maintenances.find((maintenance) =>
      isInRegion(maintenance, regionId),
    );

  if (!event) {
    return (
      <Alert
        data-testid="support-status-banner"
        role="status"
        variant="info"
        minWidth="0"
        showLabel={false}
        message={`All systems operational${regionId ? ` in ${regionId}` : ""}`}
        showButton
        buttonText="Status page"
        buttonProps={statusPageButtonProps(STATUS_PAGE_URL)}
      />
    );
  }

  return (
    <Alert
      data-testid="support-status-banner"
      role="status"
      variant={isIncident(event) ? "error" : "warning"}
      minWidth="0"
      label={isIncident(event) ? "Ongoing incident" : "Maintenance in progress"}
      message={event.name}
      showButton
      buttonText="Status page"
      buttonProps={statusPageButtonProps(event.url)}
    />
  );
};

/**
 * Live status from incident.io. While it loads, or if it fails, only a link
 * to the status page is shown, never an assumed "operational" state.
 */
const SupportStatusBanner = () => {
  const statusLink = <StatusLink />;
  return (
    <ErrorBoundary fallback={statusLink}>
      <Suspense fallback={statusLink}>
        <SupportStatusBannerInner />
      </Suspense>
    </ErrorBoundary>
  );
};

const OrganizationSummary = ({
  organizationId,
}: {
  organizationId?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { organization } = useCurrentOrganization();

  return (
    <HStack spacing="2" flexWrap="wrap" color={colors.foreground.secondary}>
      {organization?.name && (
        <Text textStyle="text-ui-reg">{organization.name}</Text>
      )}
      {organizationId && (
        <HStack
          spacing="1"
          pl="2"
          borderWidth="1px"
          borderColor={colors.border.secondary}
          borderRadius="md"
          color={colors.foreground.primary}
        >
          <Text
            textStyle="monospace"
            fontSize="xs"
            data-testid="support-org-id"
          >
            Org ID {organizationId}
          </Text>
          <CopyButton contents={organizationId} size="xs" />
        </HStack>
      )}
    </HStack>
  );
};

const AnswerItem = ({
  title,
  description,
  href,
  onClick,
}: {
  title: string;
  description: string;
  href: string;
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing="0" align="start" px="4" py="3">
      <TextLink
        href={href}
        onClick={onClick}
        {...EXTERNAL_LINK_PROPS}
        textStyle="text-ui-med"
      >
        {title}
      </TextLink>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {description}
      </Text>
    </VStack>
  );
};

const SupportPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const organizationId = useMaybeCurrentOrganizationId()?.data;
  const isIntercomIdentified = useIsIntercomIdentified();
  const hasIntercomJwt = useHasIntercomJwt();
  const unreadCount = useIntercomUnreadCount();
  const outageHint = isIntercomIdentified
    ? "For an outage, choose “Report Production Outage” in the chat. SLA plans only."
    : "Chat is unavailable, so these open an email to support.";

  return (
    <MainContentContainer>
      <PageHeader boxProps={{ flexWrap: "wrap", gap: "4" }}>
        <VStack spacing="1.5" align="start">
          <PageHeading>Help and support</PageHeading>
          <OrganizationSummary organizationId={organizationId} />
        </VStack>
        <HStack spacing="2" flexWrap="wrap">
          <Button
            as="a"
            href={SUPPORT_MAILTO}
            onClick={preferIntercom(
              hasIntercomJwt,
              openIntercomNewConversation,
            )}
            variant="primary"
            size="sm"
          >
            Open a ticket
          </Button>
          <Button
            as="a"
            href={OUTAGE_MAILTO}
            onClick={preferIntercom(
              hasIntercomJwt,
              openIntercomNewConversation,
            )}
            variant="outline"
            size="sm"
            color={colors.accent.red}
            borderColor={colors.border.error}
          >
            Report a production outage
          </Button>
          <Tooltip label={outageHint} lineHeight={1.2} hasArrow>
            <Box
              as="span"
              tabIndex={0}
              aria-label={outageHint}
              data-testid="support-outage-hint"
              display="inline-flex"
              borderRadius="full"
            >
              <InfoIcon />
            </Box>
          </Tooltip>
        </HStack>
      </PageHeader>

      <VStack spacing="5" align="stretch">
        <SupportStatusBanner />

        <Flex gap="5" flexWrap="wrap" align="flex-start">
          <VStack flex="999 1 520px" minW="0" spacing="4" align="stretch">
            <Card as="section" aria-label="Your tickets">
              <CardTitle as="h2" fontSize="md" pb="0">
                <HStack as="span" spacing="2" flexWrap="wrap">
                  <span>Your tickets</span>
                  {unreadCount > 0 && (
                    <Tag
                      size="sm"
                      borderRadius="full"
                      bg={colors.background.error}
                      color={colors.accent.red}
                      fontWeight="600"
                      data-testid="support-unread-count"
                    >
                      {unreadCount} new{" "}
                      {unreadCount === 1 ? "reply" : "replies"}
                    </Tag>
                  )}
                </HStack>
              </CardTitle>
              <CardContent>
                <VStack spacing="3" align="start">
                  <Text
                    textStyle="text-ui-reg"
                    color={colors.foreground.secondary}
                  >
                    Tickets for your organization, including ones your teammates
                    opened. Reply, add files, or check status.
                  </Text>
                  <HStack spacing="2" flexWrap="wrap">
                    <Button
                      as="a"
                      href={HELP_CENTER_TICKETS_URL}
                      {...EXTERNAL_LINK_PROPS}
                      variant="secondary"
                      size="sm"
                    >
                      View tickets
                    </Button>
                    <Button
                      as="a"
                      href={HELP_CENTER_URL}
                      {...EXTERNAL_LINK_PROPS}
                      variant="outline"
                      size="sm"
                    >
                      Open Help Center ↗
                    </Button>
                  </HStack>
                </VStack>
              </CardContent>
            </Card>

            <Card as="section" aria-label="Get answers now">
              <CardTitle as="h2" fontSize="md" pb="1">
                Get answers now
              </CardTitle>
              <VStack
                spacing="0"
                align="stretch"
                divider={<StackDivider borderColor={colors.border.primary} />}
              >
                <AnswerItem
                  title="Ask Matty"
                  description="AI answers from the docs, any time"
                  href={HELP_CENTER_URL}
                  onClick={preferIntercom(
                    hasIntercomJwt,
                    openIntercomMessenger,
                  )}
                />
                <AnswerItem
                  title="Documentation ↗"
                  description="Guides, SQL reference, troubleshooting"
                  href={docUrls["/docs/"]}
                />
                <AnswerItem
                  title="Community Slack ↗"
                  description="Ask other Materialize users"
                  href={COMMUNITY_SLACK_URL}
                />
              </VStack>
            </Card>
          </VStack>

          <VStack
            as="aside"
            aria-label="Before you open a ticket"
            flex="1 1 260px"
            minW="0"
            spacing="3"
            align="stretch"
          >
            <Card>
              <CardContent>
                <VStack spacing="2.5" align="start">
                  <Text as="h2" textStyle="text-ui-med">
                    Before you open a ticket
                  </Text>
                  <Text
                    textStyle="text-small"
                    color={colors.foreground.secondary}
                  >
                    Include your org ID, the region, and the name of the cluster
                    or object. Then pick a severity:
                  </Text>
                  <Grid
                    as="dl"
                    templateColumns="64px minmax(0, 1fr)"
                    columnGap="2.5"
                    rowGap="1.5"
                    textStyle="text-small"
                  >
                    {SEVERITIES.map(({ level, description, isUrgent }) => (
                      <React.Fragment key={level}>
                        <Text
                          as="dt"
                          fontWeight="600"
                          color={isUrgent ? colors.accent.red : undefined}
                        >
                          {level}
                        </Text>
                        <Text as="dd">{description}</Text>
                      </React.Fragment>
                    ))}
                  </Grid>
                  <TextLink
                    href={SUPPORT_PLANS_URL}
                    {...EXTERNAL_LINK_PROPS}
                    textStyle="text-small"
                    fontWeight="500"
                  >
                    Support plans and response times ↗
                  </TextLink>
                </VStack>
              </CardContent>
            </Card>
            <Card>
              <CardContent>
                <VStack spacing="1.5" align="start">
                  <Text as="h2" textStyle="text-ui-med">
                    Prefer email?
                  </Text>
                  <TextLink
                    href={SUPPORT_MAILTO}
                    textStyle="text-small"
                    fontWeight="500"
                  >
                    {SUPPORT_EMAIL}
                  </TextLink>
                  <Text
                    textStyle="text-small"
                    color={colors.foreground.secondary}
                  >
                    Send from your login address. Put &quot;urgent&quot; in the
                    subject if production is down.
                  </Text>
                </VStack>
              </CardContent>
            </Card>
          </VStack>
        </Flex>
      </VStack>
    </MainContentContainer>
  );
};

export default SupportPage;
