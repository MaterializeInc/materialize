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
  Navigate,
  useMatch,
  useNavigate,
  useSearchParams,
} from "react-router-dom";

import {
  useCurrentOrganization,
  useMaybeCurrentOrganizationId,
} from "~/api/auth";
import { isIncident } from "~/api/incident-io/types";
import { Alert } from "~/components/Alert";
import { Card, CardContent, CardTitle } from "~/components/cardComponents";
import { isInRegion } from "~/components/IncidentStatusWidget/utils";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import {
  HELP_CENTER_TICKETS_URL,
  HELP_CENTER_URL,
  STATUS_PAGE_URL,
  SUPPORT_CHAT_URL,
  SUPPORT_EMAIL,
  SUPPORT_PLANS_URL,
} from "~/externalUrls";
import { useFlags } from "~/hooks/useFlags";
import {
  isIntercomIdentifiedWith,
  openIntercomMessenger,
  openIntercomNewConversation,
  useCurrentIntercomJwt,
  useIntercomUnreadCount,
  useIsIntercomIdentified,
} from "~/hooks/useIntercom";
import { useToast } from "~/hooks/useToast";
import { InfoIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import docUrls from "~/mz-doc-urls.json";
import {
  NEW_SUPPORT_TICKET_PATH,
  SUPPORT_PAGE_PATH,
  SUPPORT_TICKET_PATH,
  supportTicketPath,
} from "~/platform/routeHelpers";
import { useSummary } from "~/queries/incident-io";
import { currentRegionIdAtom } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { EXTERNAL_LINK_PROPS } from "./externalLink";
import { NewSupportTicketDrawer } from "./NewSupportTicketDrawer";
import { isTicketsUnavailableError, useSupportTickets } from "./queries";
import { SupportTicketModal, SupportTicketsTable } from "./SupportTickets";

export const SUPPORT_MAILTO = `mailto:${SUPPORT_EMAIL}`;
export const OUTAGE_MAILTO = `${SUPPORT_MAILTO}?subject=${encodeURIComponent(
  "Urgent: production outage",
)}`;

type OpenMessenger = (onUnavailable: () => void) => boolean;

/**
 * Where outage reports go while the ticket form is off: the Messenger's
 * outage workflow when Intercom is running, otherwise an email to support.
 */
type OutageLink = {
  href: string;
  onClick: (event: React.MouseEvent<HTMLElement>) => void;
};

/** The outage form. */
const NEW_OUTAGE_TICKET_PATH = `${NEW_SUPPORT_TICKET_PATH}?severity=urgent`;

/**
 * Returns click handlers that open the Intercom Messenger instead of
 * following the link when the Messenger is booted with the signed-in user's
 * JWT.
 * Otherwise the link's href (email or the Help Center) is the fallback,
 * including when the Messenger reports booted but never appears. This checks
 * at click time, so a late boot is picked up without waiting for the next
 * readiness check.
 *
 * A Messenger that never appears is only noticed seconds after the click, too
 * late to open a new tab without the popup blocker stepping in, so a new-tab
 * fallback is offered as a link in a toast instead.
 */
const usePreferIntercom = (jwt: string | undefined) => {
  const toast = useToast({ status: "error", duration: 10_000 });
  return (open: OpenMessenger) => (event: React.MouseEvent<HTMLElement>) => {
    const link = event.currentTarget;
    const followLink = () => {
      const href = link.getAttribute("href");
      if (!href) return;
      if (link.getAttribute("target") !== "_blank") {
        window.location.assign(href);
        return;
      }
      toast({
        isClosable: true,
        description: (
          <>
            Chat didn&apos;t open.{" "}
            <TextLink href={href} isExternal>
              Continue in a new tab ↗
            </TextLink>
          </>
        ),
      });
    };
    if (isIntercomIdentifiedWith(jwt) && open(followLink)) {
      event.preventDefault();
    }
  };
};

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
    isExternal
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
        isExternal
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
  const { organization } = useCurrentOrganization();
  const isIntercomIdentified = useIsIntercomIdentified();
  const intercomJwt = useCurrentIntercomJwt();
  const unreadCount = useIntercomUnreadCount();
  const preferIntercom = usePreferIntercom(intercomJwt);
  const {
    query: ticketsQuery,
    tickets: loadedTickets,
    isLoadingMore,
    isLoadMoreError,
    isTruncated,
  } = useSupportTickets();
  // The ticket endpoints aren't deployed or configured everywhere yet. Until
  // they answer, "Open a ticket" uses the Messenger and the tickets card
  // links to Help Center. A failed refetch, or a failed later page, keeps the
  // tickets already loaded.
  const areTicketsAvailable = ticketsQuery.data !== undefined;
  const tickets = loadedTickets ?? [];
  // 404 and 503 mean the endpoints aren't deployed or configured here, so the
  // card links to Help Center. Anything else is worth retrying.
  const isTicketsLoadError =
    !areTicketsAvailable &&
    ticketsQuery.isError &&
    !isTicketsUnavailableError(ticketsQuery.error);
  const flags = useFlags();
  // The form needs the ticket endpoints, so it only replaces the Messenger
  // once they answer, and is behind its own flag until launch.
  const isTicketFormEnabled =
    Boolean(flags["support-ticket-form-cns-201"]) && areTicketsAvailable;
  const outageLink: OutageLink = {
    href: OUTAGE_MAILTO,
    // Checks for the Messenger at click time, so one that boots late keeps
    // working.
    onClick: preferIntercom(openIntercomNewConversation),
  };
  // The form and an open ticket have their own URLs, so a reload keeps the
  // user's place and other pages can link straight to them.
  const navigate = useNavigate();
  const isSupportRoute = useMatch(SUPPORT_PAGE_PATH) !== null;
  const isNewTicketRoute = useMatch(NEW_SUPPORT_TICKET_PATH) !== null;
  const selectedTicketId =
    useMatch(SUPPORT_TICKET_PATH)?.params.ticketId ?? null;
  // Replace rather than push, so Back doesn't reopen what was just closed.
  // Not navigate(-1): after a deep link, that would leave the console.
  const backToSupport = () => navigate(SUPPORT_PAGE_PATH, { replace: true });
  // The form can't be used without the ticket endpoints. Wait for them to
  // answer before sending a link to it back to the page.
  const isNewTicketUnavailable =
    isNewTicketRoute && !isTicketFormEnabled && !ticketsQuery.isPending;
  const [searchParams] = useSearchParams();
  const isOutageForm = searchParams.get("severity") === "urgent";
  const outageHint = isTicketFormEnabled
    ? "Opens the outage form. Plans with an SLA will page on-call right away."
    : isIntercomIdentified
      ? "For an outage, choose “Report Production Outage” in the chat. SLA plans only."
      : "Chat is unavailable, so these open an email to support.";

  if (
    isNewTicketUnavailable ||
    (!isSupportRoute && !isNewTicketRoute && selectedTicketId === null)
  ) {
    return <Navigate to={SUPPORT_PAGE_PATH} replace />;
  }

  return (
    <MainContentContainer>
      <PageHeader boxProps={{ flexWrap: "wrap", gap: "4" }}>
        <PageHeading>Help and support</PageHeading>
        <VStack
          spacing="2"
          align="flex-end"
          data-testid="support-ticket-actions"
        >
          <HStack spacing="2" flexWrap="wrap">
            {isTicketFormEnabled ? (
              <Button
                variant="primary"
                size="sm"
                onClick={() => navigate(NEW_SUPPORT_TICKET_PATH)}
              >
                Open a ticket
              </Button>
            ) : (
              <Button
                as="a"
                href={SUPPORT_MAILTO}
                onClick={preferIntercom(openIntercomNewConversation)}
                variant="primary"
                size="sm"
              >
                Open a ticket
              </Button>
            )}
            {isTicketFormEnabled ? (
              <Button
                onClick={() => navigate(NEW_OUTAGE_TICKET_PATH)}
                variant="outline"
                size="sm"
                color={colors.accent.red}
                borderColor={colors.border.error}
              >
                Report a production outage
              </Button>
            ) : (
              <Button
                as="a"
                href={outageLink.href}
                onClick={outageLink.onClick}
                variant="outline"
                size="sm"
                color={colors.accent.red}
                borderColor={colors.border.error}
              >
                Report a production outage
              </Button>
            )}
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
          <TextLink
            href={SUPPORT_PLANS_URL}
            isExternal
            textStyle="text-small"
            fontWeight="500"
          >
            Support plans and response times ↗
          </TextLink>
        </VStack>
      </PageHeader>

      <VStack spacing="5" align="stretch">
        <SupportStatusBanner />

        <Flex gap="5" flexWrap="wrap" align="flex-start">
          <VStack flex="999 1 520px" minW="0" spacing="4" align="stretch">
            <Card as="section" aria-label="Your tickets">
              <CardTitle
                as="h2"
                fontSize="md"
                pb="0"
                display="flex"
                alignItems="center"
                justifyContent="space-between"
                gap="2"
                flexWrap="wrap"
              >
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
                {areTicketsAvailable && (
                  <TextLink
                    href={HELP_CENTER_TICKETS_URL}
                    isExternal
                    textStyle="text-small"
                    fontWeight="500"
                  >
                    View in Help Center ↗
                  </TextLink>
                )}
              </CardTitle>
              <CardContent>
                {ticketsQuery.isPending ||
                (tickets.length === 0 && isLoadingMore) ? (
                  <LoadingContainer minHeight="24" />
                ) : areTicketsAvailable ? (
                  <VStack spacing="3" align="stretch">
                    <SupportTicketsTable
                      tickets={tickets}
                      // Pages that didn't load may hold tickets, so don't
                      // say there are none.
                      isComplete={!isTruncated && !isLoadMoreError}
                      isPartial={isLoadingMore || isLoadMoreError}
                      onSelect={(ticketId) =>
                        navigate(supportTicketPath(ticketId))
                      }
                    />
                    {isLoadingMore && (
                      <Text
                        textStyle="text-small"
                        color={colors.foreground.secondary}
                        data-testid="support-tickets-loading-more"
                      >
                        Loading more tickets…
                      </Text>
                    )}
                    {isLoadMoreError && (
                      <HStack spacing="3" flexWrap="wrap">
                        <Text
                          textStyle="text-small"
                          color={colors.foreground.secondary}
                          data-testid="support-tickets-load-more-error"
                        >
                          Some of your tickets couldn&apos;t be loaded.
                        </Text>
                        <Button
                          variant="secondary"
                          size="sm"
                          onClick={() => ticketsQuery.fetchNextPage()}
                          isLoading={ticketsQuery.isFetchingNextPage}
                        >
                          Try again
                        </Button>
                      </HStack>
                    )}
                    {isTruncated && (
                      <Text
                        textStyle="text-small"
                        color={colors.foreground.secondary}
                        data-testid="support-tickets-truncated"
                      >
                        Older tickets are in{" "}
                        <TextLink href={HELP_CENTER_TICKETS_URL} isExternal>
                          Help Center
                        </TextLink>
                        .
                      </Text>
                    )}
                  </VStack>
                ) : isTicketsLoadError ? (
                  <VStack spacing="3" align="start">
                    <Alert
                      variant="error"
                      minWidth="0"
                      message="Your tickets couldn't be loaded. You can still view them in Help Center."
                    />
                    <HStack spacing="2" flexWrap="wrap">
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => ticketsQuery.refetch()}
                        isLoading={ticketsQuery.isRefetching}
                      >
                        Try again
                      </Button>
                      <Button
                        as="a"
                        href={HELP_CENTER_TICKETS_URL}
                        {...EXTERNAL_LINK_PROPS}
                        variant="outline"
                        size="sm"
                      >
                        View in Help Center ↗
                      </Button>
                    </HStack>
                  </VStack>
                ) : (
                  <VStack spacing="3" align="start">
                    <Text
                      textStyle="text-ui-reg"
                      color={colors.foreground.secondary}
                    >
                      See the tickets you opened, reply, add files, or check
                      status in Help Center.
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
                )}
              </CardContent>
            </Card>
          </VStack>

          <VStack
            as="aside"
            aria-label="More ways to get help"
            flex="1 1 260px"
            minW="0"
            spacing="3"
            align="stretch"
          >
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
                  onClick={preferIntercom(openIntercomMessenger)}
                />
                <AnswerItem
                  title="Documentation ↗"
                  description="Guides, SQL reference, troubleshooting"
                  href={docUrls["/docs/"]}
                />
                <AnswerItem
                  title="Community Slack ↗"
                  description="Ask other Materialize users"
                  href={SUPPORT_CHAT_URL}
                />
              </VStack>
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
      <NewSupportTicketDrawer
        isOpen={isNewTicketRoute && isTicketFormEnabled}
        onClose={backToSupport}
        isOutage={isOutageForm}
        onReportOutage={() =>
          navigate(NEW_OUTAGE_TICKET_PATH, { replace: true })
        }
        organizationId={organizationId}
        organizationName={organization?.name}
      />
      <SupportTicketModal ticketId={selectedTicketId} onClose={backToSupport} />
    </MainContentContainer>
  );
};

export default SupportPage;
