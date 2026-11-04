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
  Tag,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { createColumnHelper, FilterFn } from "@tanstack/react-table";
import React from "react";
import ReactSelect from "react-select";

import { SupportTicket } from "~/api/cloudGlobalApi";
import { Alert } from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { HELP_CENTER_TICKETS_URL } from "~/externalUrls";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { EXTERNAL_LINK_PROPS } from "./externalLink";
import { useListedSupportTicket, useSupportTicket } from "./queries";
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

/** Colors a ticket's state by its category, the way the Help Center groups them. */
const useStateColors = (ticket: SupportTicket) => {
  const { colors } = useTheme<MaterializeTheme>();
  switch (ticketCategory(ticket)) {
    case "resolved":
      return { bg: colors.background.tertiary, color: colors.accent.green };
    case "waiting_on_customer":
      // The one state that needs the customer, so it stands out.
      return { bg: colors.background.warn, color: colors.foreground.primary };
    case "in_progress":
      return { bg: colors.background.info, color: colors.accent.blue };
    default:
      return {
        bg: colors.background.tertiary,
        color: colors.foreground.primary,
      };
  }
};

const TicketStateTag = ({ ticket }: { ticket: SupportTicket }) => {
  const stateColors = useStateColors(ticket);
  const isWaitingOnYou = ticketCategory(ticket) === "waiting_on_customer";
  return (
    <Tag
      size="sm"
      borderRadius="md"
      bg={stateColors.bg}
      color={stateColors.color}
      fontWeight={isWaitingOnYou ? "600" : "500"}
      whiteSpace="nowrap"
    >
      {ticket.state ?? (ticket.open ? "Open" : "Resolved")}
    </Tag>
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
  { id: "all", label: "All tickets" },
  { id: "open", label: "Open" },
  { id: "resolved", label: "Resolved" },
];

const statusFilter: FilterFn<SupportTicket> = (row, _columnId, value) =>
  value === "resolved" ? isResolved(row.original) : !isResolved(row.original);

const columnHelper = createColumnHelper<SupportTicket>();

const TicketTitleCell = ({ ticket }: { ticket: SupportTicket }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing="0" align="start" py="1">
      {/* A button so the row can be reached with the keyboard. Its click
          bubbles up to the row, which opens the ticket. */}
      <Button
        variant="link"
        color={colors.foreground.primary}
        fontWeight="500"
        textAlign="left"
        whiteSpace="normal"
      >
        {ticket.title}
      </Button>
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

const columns = [
  columnHelper.accessor("title", {
    header: "Ticket",
    sortingFn: "alphanumeric",
    cell: (info) => <TicketTitleCell ticket={info.row.original} />,
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

const TicketStatusFilter = ({
  value,
  onChange,
}: {
  value: StatusFilter | undefined;
  onChange: (value: StatusFilter | undefined) => void;
}) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  type FilterOption = (typeof STATUS_FILTER_OPTIONS)[number];
  return (
    <ReactSelect<FilterOption, false>
      aria-label="Filter tickets by status"
      components={{ Option, DropdownIndicator }}
      isMulti={false}
      isSearchable={false}
      getOptionValue={(option) => option.id}
      options={STATUS_FILTER_OPTIONS}
      value={
        STATUS_FILTER_OPTIONS.find((option) => option.id === value) ??
        STATUS_FILTER_OPTIONS[0]
      }
      onChange={(option) =>
        onChange(
          !option || option.id === "all"
            ? undefined
            : (option.id as StatusFilter),
        )
      }
      styles={buildReactSelectFilterStyles<FilterOption, false>({
        colors,
        shadows,
      })}
    />
  );
};

export interface SupportTicketsTableProps {
  tickets: SupportTicket[];
  onSelect: (ticketId: string) => void;
}

/**
 * The signed-in user's loaded tickets, most recently updated first. Sorting
 * and the status filter apply to the tickets loaded so far.
 */
export const SupportTicketsTable = ({
  tickets,
  onSelect,
}: SupportTicketsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const table = useUniversalTable({
    data: tickets,
    columns,
    initialSorting: [{ id: "updatedAt", desc: true }],
    // Paging happens against the API with "Load more".
    pageSize: Number.MAX_SAFE_INTEGER,
  });
  const statusColumn = table.getColumn("status");
  const filterValue = statusColumn?.getFilterValue() as
    StatusFilter | undefined;

  if (tickets.length === 0) {
    return (
      <Text
        textStyle="text-ui-reg"
        color={colors.foreground.secondary}
        data-testid="support-tickets-empty"
      >
        You haven&apos;t opened any tickets yet.
      </Text>
    );
  }

  const shownCount = table.getRowModel().rows.length;

  return (
    <VStack spacing="2" align="stretch">
      <Box alignSelf="start">
        <TicketStatusFilter
          value={filterValue}
          onChange={(value) => statusColumn?.setFilterValue(value)}
        />
      </Box>
      <Box overflowX="auto" width="100%">
        <UniversalTable
          table={table}
          onRowClick={(ticket) => onSelect(ticket.id)}
          data-testid="support-tickets-table"
        />
      </Box>
      {shownCount === 0 && (
        <Text
          textStyle="text-small"
          color={colors.foreground.secondary}
          data-testid="support-tickets-none-match"
        >
          No {filterValue === "resolved" ? "resolved" : "open"} tickets loaded.
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
                <MessageBubble
                  author="You"
                  body={htmlToParts(data.description)}
                  createdAt={data.ticket.createdAt}
                  fromYou
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
