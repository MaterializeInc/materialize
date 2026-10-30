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
  Table,
  Tag,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { SupportTicket } from "~/api/cloudGlobalApi";
import { Alert } from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import { HELP_CENTER_TICKETS_URL } from "~/externalUrls";
import { MaterializeTheme } from "~/theme";

import { useSupportTicket } from "./queries";
import { formatRelativeTime, htmlToText } from "./supportTicketUtils";

const EXTERNAL_LINK_PROPS = {
  target: "_blank",
  rel: "noopener noreferrer",
} as const;

/** Colors a ticket's state the way the Help Center groups them. */
const useStateColors = (ticket: SupportTicket) => {
  const { colors } = useTheme<MaterializeTheme>();
  const state = (ticket.state ?? "").toLowerCase();
  if (!ticket.open || /resolved|closed/.test(state)) {
    return { bg: colors.green[50], color: colors.green[600] };
  }
  if (/waiting on you|waiting on customer/.test(state)) {
    return { bg: colors.background.warn, color: colors.foreground.primary };
  }
  if (/progress/.test(state)) {
    return { bg: colors.background.info, color: colors.accent.blue };
  }
  return { bg: colors.background.tertiary, color: colors.foreground.primary };
};

const TicketStateTag = ({ ticket }: { ticket: SupportTicket }) => {
  const stateColors = useStateColors(ticket);
  return (
    <Tag
      size="sm"
      borderRadius="md"
      bg={stateColors.bg}
      color={stateColors.color}
      fontWeight="500"
      whiteSpace="nowrap"
    >
      {ticket.state ?? (ticket.open ? "Open" : "Closed")}
    </Tag>
  );
};

export interface SupportTicketsTableProps {
  tickets: SupportTicket[];
  onSelect: (ticketId: string) => void;
}

/** The signed-in user's tickets, most recently updated first. */
export const SupportTicketsTable = ({
  tickets,
  onSelect,
}: SupportTicketsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();

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

  return (
    <Box overflowX="auto" width="100%">
      <Table variant="linkable" size="sm" minWidth="480px">
        <Thead>
          <Tr>
            <Th>Ticket</Th>
            <Th>Status</Th>
            <Th>Updated</Th>
          </Tr>
        </Thead>
        <Tbody>
          {tickets.map((ticket) => (
            <Tr key={ticket.id}>
              <Td py="2.5">
                <VStack spacing="0" align="start">
                  <Button
                    variant="link"
                    color={colors.foreground.primary}
                    fontWeight="500"
                    textAlign="left"
                    whiteSpace="normal"
                    onClick={() => onSelect(ticket.id)}
                  >
                    {ticket.title}
                  </Button>
                  {ticket.number && (
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      #{ticket.number} · opened by you
                    </Text>
                  )}
                </VStack>
              </Td>
              <Td py="2.5">
                <TicketStateTag ticket={ticket} />
              </Td>
              <Td
                py="2.5"
                color={colors.foreground.secondary}
                whiteSpace="nowrap"
              >
                {formatRelativeTime(ticket.updatedAt)}
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  );
};

const MessageBubble = ({
  author,
  body,
  createdAt,
  fromYou,
}: {
  author: string;
  body: string;
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
          {formatRelativeTime(createdAt)}
        </Text>
      </HStack>
      <Text textStyle="text-ui-reg" whiteSpace="pre-wrap">
        {body}
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

  return (
    <Modal isOpen={ticketId !== null} onClose={onClose} size="2xl">
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <ModalHeader pr="12">
          {data ? data.ticket.title : "Ticket"}
          {data?.ticket.number && (
            <Text
              as="span"
              textStyle="text-ui-reg"
              color={colors.foreground.secondary}
              ml="2"
            >
              #{data.ticket.number}
            </Text>
          )}
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
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
              <HStack spacing="2">
                <TicketStateTag ticket={data.ticket} />
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  Opened {formatRelativeTime(data.ticket.createdAt)}
                </Text>
              </HStack>
              {data.description && (
                <MessageBubble
                  author="You"
                  body={htmlToText(data.description)}
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
                  body={htmlToText(message.bodyHtml)}
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
