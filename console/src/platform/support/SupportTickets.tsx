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
  HStack,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  Tooltip,
  useTheme,
  VisuallyHidden,
  VStack,
} from "@chakra-ui/react";
import { createColumnHelper, FilterFn } from "@tanstack/react-table";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { SupportTicket } from "~/api/cloudGlobalApi";
import { Alert } from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import SimpleSelect from "~/components/SimpleSelect";
import StatusPill from "~/components/StatusPill";
import { TablePagination } from "~/components/Table/TablePagination";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { useAuth, useAuthUser } from "~/external-library-wrappers/frontegg";
import { HELP_CENTER_TICKETS_URL } from "~/externalUrls";
import { supportTicketPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { AttachFilesStatus } from "./AttachFilesStatus";
import { EXTERNAL_LINK_PROPS } from "./externalLink";
import { useListedSupportTicket, useSupportTicket } from "./queries";
import { SegmentedControl, SegmentOption } from "./SegmentedControl";
import {
  formatRelativeTime,
  htmlToParts,
  MessagePart,
} from "./supportTicketUtils";

/**
 * A ticket's state category: Intercom's internal state, which stays the same
 * when support renames the label customers see.
 */
type StateCategory = NonNullable<SupportTicket["stateCategory"]>;

const ticketCategory = (ticket: SupportTicket): StateCategory | undefined =>
  ticket.stateCategory ?? (ticket.open ? undefined : "resolved");

const isResolved = (ticket: SupportTicket) =>
  ticketCategory(ticket) === "resolved";

const ticketStateColorScheme = (
  ticket: SupportTicket,
): "green" | "yellow" | "blue" | "gray" => {
  switch (ticketCategory(ticket)) {
    case "resolved":
      return "green";
    case "waiting_on_customer":
      return "yellow";
    case "in_progress":
      return "blue";
    default:
      return "gray";
  }
};

const TicketStateTag = ({ ticket }: { ticket: SupportTicket }) => {
  return (
    <StatusPill
      status={ticket.state ?? (ticket.open ? "Open" : "Resolved")}
      colorScheme={ticketStateColorScheme(ticket)}
      whiteSpace="nowrap"
    />
  );
};

/** "2 hours ago", with the full date and time on hover. */
export const RelativeTime = ({
  date,
  prefix,
}: {
  date: string;
  prefix?: string;
}) => (
  <Tooltip
    label={formatDate(new Date(date), FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
    hasArrow
    openDelay={300}
  >
    <Box as="time" dateTime={date} tabIndex={0}>
      {prefix}
      {formatRelativeTime(date)}
    </Box>
  </Tooltip>
);

type StatusFilter = "open" | "resolved";

const STATUS_FILTER_OPTIONS: { id: StatusFilter | "all"; label: string }[] = [
  { id: "open", label: "Open" },
  { id: "resolved", label: "Resolved" },
  { id: "all", label: "All tickets" },
];

/** Tickets shown per page of the table. */
const TICKETS_PER_PAGE = 10;

const statusFilter: FilterFn<SupportTicket> = (row, _columnId, value) =>
  value === "resolved" ? isResolved(row.original) : !isResolved(row.original);

const columnHelper = createColumnHelper<SupportTicket>();

const TicketTitleCell = ({ ticket }: { ticket: SupportTicket }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing="0" align="start" py="1">
      {/* A link, so it reads as clickable, can be reached with the keyboard
          and opened in a new tab. The row opens the same ticket, so the
          link's click doesn't also reach the row. */}
      <TextLink
        as={RouterLink}
        to={supportTicketPath(ticket.id)}
        textStyle="text-ui-reg"
        onClick={(event: React.MouseEvent) => event.stopPropagation()}
      >
        {ticket.title}
      </TextLink>
      {ticket.number && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          #{ticket.number}
        </Text>
      )}
    </VStack>
  );
};

const UpdatedCell = ({ ticket }: { ticket: SupportTicket }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text
      as="span"
      color={colors.foreground.secondary}
      whiteSpace="nowrap"
      textStyle="text-small"
    >
      <RelativeTime date={ticket.updatedAt} />
    </Text>
  );
};

/** For a value only known for tickets opened from the console. */
const UnknownValue = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text as="span" textStyle="text-small" color={colors.foreground.tertiary}>
      —
    </Text>
  );
};

/**
 * The organization's name for organizations the user belongs to. Other
 * organizations show the start of their ID, with the full ID on hover.
 */
const OrganizationCell = ({
  organizationId,
  name,
}: {
  organizationId: string | null | undefined;
  name: string | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!organizationId) return <UnknownValue />;
  if (name) {
    return (
      <Text
        as="span"
        textStyle="text-small"
        color={colors.foreground.secondary}
      >
        {name}
      </Text>
    );
  }
  return (
    <Text
      as="span"
      textStyle="monospace"
      fontSize="xs"
      color={colors.foreground.secondary}
      title={organizationId}
    >
      {organizationId.slice(0, 8)}
    </Text>
  );
};

const OpenedByCell = ({
  email,
  isYou,
}: {
  email: string | null | undefined;
  isYou: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!email) return <UnknownValue />;
  return (
    <Text
      as="span"
      textStyle="text-small"
      color={colors.foreground.secondary}
      title={isYou ? email : undefined}
    >
      {isYou ? "You" : email}
    </Text>
  );
};

/** What the table's cells need from the signed-in user. */
type SupportTicketsTableMeta = {
  /** Names of the organizations the user belongs to, by ID. */
  organizationNames: Map<string, string>;
  userEmail: string;
};

const columns = [
  columnHelper.accessor("title", {
    header: "Ticket",
    sortingFn: "alphanumeric",
    cell: (info) => <TicketTitleCell ticket={info.row.original} />,
  }),
  columnHelper.accessor((ticket) => ticket.organizationId ?? "", {
    id: "organization",
    header: "Organization",
    // Sorting by ID wouldn't follow the names shown.
    enableSorting: false,
    filterFn: (row, _columnId, value) => row.original.organizationId === value,
    cell: (info) => {
      const { organizationNames } = info.table.options
        .meta as SupportTicketsTableMeta;
      const { organizationId } = info.row.original;
      return (
        <OrganizationCell
          organizationId={organizationId}
          name={
            organizationId ? organizationNames.get(organizationId) : undefined
          }
        />
      );
    },
  }),
  columnHelper.accessor((ticket) => ticket.openedBy ?? "", {
    id: "openedBy",
    header: "Opened by",
    sortingFn: "alphanumeric",
    cell: (info) => {
      const { userEmail } = info.table.options.meta as SupportTicketsTableMeta;
      return (
        <OpenedByCell
          email={info.row.original.openedBy}
          isYou={isSameEmail(info.row.original.openedBy, userEmail)}
        />
      );
    },
  }),
  columnHelper.accessor((ticket) => ticket.state ?? "", {
    id: "status",
    header: "Status",
    sortingFn: "alphanumeric",
    filterFn: statusFilter,
    cell: (info) => <TicketStateTag ticket={info.row.original} />,
  }),
  columnHelper.accessor("updatedAt", {
    header: "Updated",
    sortingFn: "basic",
    cell: (info) => <UpdatedCell ticket={info.row.original} />,
  }),
];

const isSameEmail = (a: string | null | undefined, b: string) =>
  Boolean(a) && a?.toLowerCase() === b.toLowerCase();

/** Open, Resolved or All tickets, as a row of radio buttons. */
const TicketStatusFilter = ({
  value,
  onChange,
}: {
  value: StatusFilter | undefined;
  onChange: (value: StatusFilter | undefined) => void;
}) => {
  const selected = value ?? "all";
  return (
    <Box as="fieldset">
      <VisuallyHidden as="legend">Show tickets</VisuallyHidden>
      <SegmentedControl>
        {STATUS_FILTER_OPTIONS.map((option) => (
          <SegmentOption
            key={option.id}
            size="sm"
            value={option.id}
            label={option.label}
            isSelected={option.id === selected}
            inputProps={{
              name: "support-ticket-status",
              checked: option.id === selected,
              onChange: () =>
                onChange(option.id === "all" ? undefined : option.id),
            }}
          />
        ))}
      </SegmentedControl>
    </Box>
  );
};

export interface SupportTicketsTableProps {
  tickets: SupportTicket[];
  /**
   * Every ticket loaded. When some pages didn't load, an empty list doesn't
   * mean the user has no tickets.
   */
  isComplete: boolean;
  /**
   * Some pages are still loading or failed to load, so a filter with no
   * matches yet may have some.
   */
  isPartial: boolean;
  onSelect: (ticketId: string) => void;
}

/**
 * The signed-in user's tickets, most recently updated first. Open tickets
 * show by default; the filter, sorting and pages apply to all loaded tickets.
 */
export const SupportTicketsTable = ({
  tickets,
  isComplete,
  isPartial,
  onSelect,
}: SupportTicketsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { tenantsState } = useAuth();
  const user = useAuthUser();
  const organizationNames = new Map(
    tenantsState.tenants
      .filter((tenant) => tenant?.name)
      .map((tenant) => [tenant.tenantId, tenant.name]),
  );
  const meta: SupportTicketsTableMeta = {
    organizationNames,
    userEmail: user.email,
  };
  const table = useUniversalTable({
    data: tickets,
    columns,
    // The API only knows these for tickets opened from the console, and
    // older API versions don't return them, so the columns show only when
    // used.
    state: {
      columnVisibility: {
        organization: tickets.some((ticket) => ticket.organizationId),
        openedBy: tickets.some((ticket) => ticket.openedBy),
      },
    },
    meta,
    initialSorting: [{ id: "updatedAt", desc: true }],
    initialState: { columnFilters: [{ id: "status", value: "open" }] },
    pageSize: TICKETS_PER_PAGE,
  });
  const statusColumn = table.getColumn("status");
  const organizationColumn = table.getColumn("organization");
  const organizationFilter = organizationColumn?.getFilterValue() as
    string | undefined;
  // The organizations the user's tickets are for, like Help Center's company
  // filter. Tickets opened outside the console have none. The table keeps its
  // filters when the tickets change, so a selected organization stays listed,
  // and the filter shown, after a refresh drops its last ticket. Otherwise the
  // filter couldn't be cleared.
  const organizationIds = new Set(
    tickets.flatMap((ticket) =>
      ticket.organizationId ? [ticket.organizationId] : [],
    ),
  );
  if (organizationFilter) organizationIds.add(organizationFilter);
  const organizationOptions = [...organizationIds]
    .map((id) => ({ id, label: organizationNames.get(id) ?? id.slice(0, 8) }))
    .sort((a, b) => a.label.localeCompare(b.label));
  const filterValue = statusColumn?.getFilterValue() as
    StatusFilter | undefined;

  if (tickets.length === 0) {
    return (
      <Text
        textStyle="text-ui-reg"
        color={colors.foreground.secondary}
        data-testid="support-tickets-empty"
      >
        {isComplete
          ? "You haven't opened any tickets yet."
          : "No tickets to show."}
      </Text>
    );
  }

  const shownCount = table.getRowModel().rows.length;

  return (
    <VStack spacing="2" align="stretch">
      <HStack spacing="2" flexWrap="wrap" alignSelf="start">
        <TicketStatusFilter
          value={filterValue}
          onChange={(value) => statusColumn?.setFilterValue(value)}
        />
        {(organizationOptions.length > 1 || organizationFilter) && (
          <SimpleSelect
            aria-label="Filter tickets by organization"
            value={organizationFilter ?? ""}
            onChange={(event) =>
              organizationColumn?.setFilterValue(
                event.target.value || undefined,
              )
            }
          >
            <option value="">All organizations</option>
            {organizationOptions.map((option) => (
              <option key={option.id} value={option.id}>
                {option.label}
              </option>
            ))}
          </SimpleSelect>
        )}
      </HStack>
      <Box overflowX="auto" width="100%">
        <UniversalTable
          table={table}
          onRowClick={(ticket) => onSelect(ticket.id)}
          data-testid="support-tickets-table"
        />
        <TablePagination table={table} itemLabel="tickets" />
      </Box>
      {shownCount === 0 && !isPartial && (
        <Text
          textStyle="text-small"
          color={colors.foreground.secondary}
          data-testid="support-tickets-none-match"
        >
          No {filterValue === "resolved" ? "resolved" : "open"} tickets.
        </Text>
      )}
    </VStack>
  );
};

const MessageBubble = ({
  author,
  body,
  createdAt,
  fromYou,
}: {
  author: string;
  body: MessagePart[];
  createdAt: string;
  fromYou: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      alignSelf={fromYou ? "flex-end" : "flex-start"}
      maxWidth="85%"
      borderRadius="lg"
      borderWidth="1px"
      borderColor={colors.border.primary}
      bg={fromYou ? colors.background.secondary : colors.background.primary}
      px="3"
      py="2"
    >
      <HStack spacing="2" mb="1">
        <Text textStyle="text-small-heavy">{author}</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          <RelativeTime date={createdAt} />
        </Text>
      </HStack>
      <Text textStyle="text-ui-reg" whiteSpace="pre-wrap">
        {body.map((part, index) =>
          part.href ? (
            <TextLink key={index} href={part.href} isExternal>
              {part.text}
            </TextLink>
          ) : (
            <React.Fragment key={index}>{part.text}</React.Fragment>
          ),
        )}
      </Text>
    </Box>
  );
};

export interface SupportTicketModalProps {
  ticketId: string | null;
  onClose: () => void;
}

/** A ticket's description and replies. Replying happens in Help Center. */
export const SupportTicketModal = ({
  ticketId,
  onClose,
}: SupportTicketModalProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const user = useAuthUser();
  const { data, isPending, isError } = useSupportTicket(ticketId);
  // The list already has the title and state, so only the replies wait.
  const listed = useListedSupportTicket(ticketId);
  const ticket = data?.ticket ?? listed;

  return (
    <Modal isOpen={ticketId !== null} onClose={onClose} size="2xl">
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <ModalHeader pr="12">
          {ticket ? ticket.title : "Ticket"}
          {ticket?.number && (
            <Text
              as="span"
              textStyle="text-ui-reg"
              color={colors.foreground.secondary}
              ml="2"
            >
              #{ticket.number}
            </Text>
          )}
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          {ticket && (
            <HStack spacing="2" mb="3">
              <TicketStateTag ticket={ticket} />
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                <RelativeTime date={ticket.createdAt} prefix="Opened " />
              </Text>
            </HStack>
          )}
          {ticketId && (
            // Files added from the console this session, while they upload
            // or if they didn't make it.
            <AttachFilesStatus ticketId={ticketId} showShareInstead mb="3" />
          )}
          {isPending ? (
            <LoadingContainer />
          ) : isError || !data ? (
            <Alert
              variant="error"
              minWidth="0"
              message="This ticket couldn't be loaded. You can still view it in Help Center."
            />
          ) : (
            <VStack align="stretch" spacing="3" data-testid="support-ticket">
              {data.description && (
                // Opened by a teammate when the console recorded someone
                // else; otherwise the user's.
                <MessageBubble
                  author={
                    data.ticket.openedBy &&
                    !isSameEmail(data.ticket.openedBy, user.email)
                      ? data.ticket.openedBy
                      : "You"
                  }
                  body={htmlToParts(data.description)}
                  createdAt={data.ticket.createdAt}
                  fromYou={
                    !data.ticket.openedBy ||
                    isSameEmail(data.ticket.openedBy, user.email)
                  }
                />
              )}
              {data.messages.map((message) => (
                <MessageBubble
                  key={message.id}
                  author={
                    message.fromYou
                      ? "You"
                      : (message.authorName ?? "Materialize support")
                  }
                  body={htmlToParts(message.bodyHtml)}
                  createdAt={message.createdAt}
                  fromYou={message.fromYou}
                />
              ))}
            </VStack>
          )}
        </ModalBody>
        <ModalFooter>
          <HStack spacing="2">
            <Button variant="secondary" size="sm" onClick={onClose}>
              Close
            </Button>
            <Button
              as="a"
              href={HELP_CENTER_TICKETS_URL}
              {...EXTERNAL_LINK_PROPS}
              variant="primary"
              size="sm"
            >
              Reply in Help Center ↗
            </Button>
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};
