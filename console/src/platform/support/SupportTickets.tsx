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
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
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
  Textarea,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React from "react";
import { useForm } from "react-hook-form";

import {
  SupportTicket,
  SupportTicketEnvironmentType,
  SupportTicketSeverity,
} from "~/api/cloudGlobalApi";
import { Alert } from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import SimpleSelect from "~/components/SimpleSelect";
import { HELP_CENTER_TICKETS_URL, SUPPORT_EMAIL } from "~/externalUrls";
import { useToast } from "~/hooks/useToast";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { useCreateSupportTicket, useSupportTicket } from "./queries";
import { formatRelativeTime, htmlToText } from "./supportTicketUtils";

const EXTERNAL_LINK_PROPS = {
  target: "_blank",
  rel: "noopener noreferrer",
} as const;

const TicketStateTag = ({ ticket }: { ticket: SupportTicket }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Tag
      size="sm"
      borderRadius="md"
      bg={ticket.open ? colors.background.info : colors.background.tertiary}
      color={colors.foreground.primary}
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
                      #{ticket.number}
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

const SEVERITY_OPTIONS: { value: SupportTicketSeverity; label: string }[] = [
  { value: "high", label: "High: key features seriously impaired" },
  { value: "normal", label: "Normal: some impact, a workaround exists" },
  { value: "low", label: "Low: little or no impact, or a question" },
];

const ENVIRONMENT_OPTIONS: {
  value: SupportTicketEnvironmentType;
  label: string;
}[] = [
  { value: "production", label: "Production" },
  { value: "staging", label: "Staging" },
  { value: "development", label: "Development" },
];

type TicketFormState = {
  title: string;
  severity: SupportTicketSeverity;
  environmentType: SupportTicketEnvironmentType;
  description: string;
};

/** Context the console knows, attached so the customer doesn't retype it. */
const useTicketContext = () => {
  const regionId = useAtomValue(currentRegionIdAtom);
  const environment = useAtomValue(currentEnvironmentState);
  const environmentVersion =
    environment?.state === "enabled" && environment.status.health === "healthy"
      ? `v${environment.status.version.crateVersion.version}`
      : undefined;
  return { region: regionId ?? undefined, environmentVersion };
};

const NewSupportTicketForm = ({
  organizationId,
  onClose,
}: {
  organizationId?: string;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const toast = useToast();
  const context = useTicketContext();
  const createTicket = useCreateSupportTicket();
  const { register, handleSubmit, formState } = useForm<TicketFormState>({
    mode: "onTouched",
    defaultValues: {
      title: "",
      severity: "normal",
      environmentType: "production",
      description: "",
    },
  });

  const onSubmit = (values: TicketFormState) => {
    createTicket.mutate(
      {
        title: values.title.trim(),
        description: values.description.trim(),
        severity: values.severity,
        environmentType: values.environmentType,
        region: context.region,
        environmentVersion: context.environmentVersion,
      },
      {
        onSuccess: (ticket) => {
          toast({
            description: ticket.number
              ? `Ticket #${ticket.number} opened. We'll reply by email and in Help Center.`
              : "Ticket opened. We'll reply by email and in Help Center.",
          });
          onClose();
        },
      },
    );
  };

  const attached = [
    organizationId && `org ID ${organizationId}`,
    context.region && `region ${context.region}`,
    context.environmentVersion && `version ${context.environmentVersion}`,
  ].filter(Boolean);

  return (
    <form onSubmit={handleSubmit(onSubmit)}>
      <ModalHeader>Open a ticket</ModalHeader>
      <ModalCloseButton />
      <ModalBody>
        <VStack spacing="4" align="stretch">
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Production down with no workaround? Close this and use{" "}
            <Text as="span" fontWeight="500">
              Report a production outage
            </Text>{" "}
            instead, so on-call is paged.
          </Text>
          {createTicket.isError && (
            <Alert
              variant="error"
              minWidth="0"
              message={`Your ticket couldn't be opened. Try again, or email ${SUPPORT_EMAIL}.`}
            />
          )}
          <FormControl isInvalid={!!formState.errors.title}>
            <FormLabel fontSize="sm">Title</FormLabel>
            <Input
              {...register("title", {
                validate: (value) =>
                  value.trim().length > 0 || "A title is required.",
                maxLength: {
                  value: 200,
                  message: "Keep the title under 200 characters.",
                },
              })}
              placeholder="e.g. Replica keeps restarting"
              autoFocus
              size="sm"
              variant={formState.errors.title ? "error" : "default"}
            />
            <FormErrorMessage>
              {formState.errors.title?.message}
            </FormErrorMessage>
          </FormControl>
          <HStack spacing="4" align="start" flexWrap="wrap">
            <FormControl flex="1 1 240px">
              <FormLabel fontSize="sm">Severity</FormLabel>
              <SimpleSelect {...register("severity")} size="sm">
                {SEVERITY_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </SimpleSelect>
            </FormControl>
            <FormControl flex="1 1 160px">
              <FormLabel fontSize="sm">Environment</FormLabel>
              <SimpleSelect {...register("environmentType")} size="sm">
                {ENVIRONMENT_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </SimpleSelect>
            </FormControl>
          </HStack>
          <FormControl isInvalid={!!formState.errors.description}>
            <FormLabel fontSize="sm">What&apos;s happening?</FormLabel>
            <Textarea
              {...register("description", {
                validate: (value) =>
                  value.trim().length > 0 || "Describe the problem.",
                maxLength: {
                  value: 10000,
                  message: "Keep the description under 10,000 characters.",
                },
              })}
              rows={6}
              size="sm"
              placeholder="What you expected, what happened instead, and the name of the cluster or object involved."
            />
            <FormErrorMessage>
              {formState.errors.description?.message}
            </FormErrorMessage>
            {attached.length > 0 && (
              <FormHelperText data-testid="support-ticket-context">
                We&apos;ll attach your {attached.join(", ")}.
              </FormHelperText>
            )}
          </FormControl>
        </VStack>
      </ModalBody>
      <ModalFooter>
        <HStack spacing="2">
          <Button variant="secondary" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button
            type="submit"
            variant="primary"
            size="sm"
            isLoading={createTicket.isPending}
          >
            Open ticket
          </Button>
        </HStack>
      </ModalFooter>
    </form>
  );
};

export interface NewSupportTicketModalProps {
  isOpen: boolean;
  onClose: () => void;
  organizationId?: string;
}

export const NewSupportTicketModal = ({
  isOpen,
  onClose,
  organizationId,
}: NewSupportTicketModalProps) => {
  const { shadows } = useTheme<MaterializeTheme>();
  return (
    <Modal isOpen={isOpen} onClose={onClose} size="xl">
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <React.Suspense
          fallback={
            <ModalBody>
              <LoadingContainer />
            </ModalBody>
          }
        >
          {isOpen && (
            <NewSupportTicketForm
              organizationId={organizationId}
              onClose={onClose}
            />
          )}
        </React.Suspense>
      </ModalContent>
    </Modal>
  );
};
