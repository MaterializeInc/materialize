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
  Checkbox,
  Drawer,
  DrawerBody,
  DrawerContent,
  DrawerFooter,
  DrawerHeader,
  DrawerOverlay,
  Flex,
  FormControl,
  FormErrorMessage,
  FormLabel,
  Grid,
  HStack,
  IconButton,
  Input,
  ListItem,
  OrderedList,
  Tag,
  Text,
  Textarea,
  useTheme,
  VisuallyHiddenInput,
  VStack,
} from "@chakra-ui/react";
import { useAtomValue } from "jotai";
import React from "react";
import { useForm, UseFormRegisterReturn } from "react-hook-form";

import {
  CreatedSupportTicket,
  CreateSupportTicketRequest,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { Alert } from "~/components/Alert";
import { HELP_CENTER_TICKETS_URL, SUPPORT_EMAIL } from "~/externalUrls";
import { useHideIntercomLauncher } from "~/hooks/useIntercom";
import { CheckmarkIcon, ChevronLeftIcon } from "~/icons";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { EXTERNAL_LINK_PROPS } from "./externalLink";
import { useCreateSupportTicket } from "./queries";

type CreateTicketMutation = ReturnType<typeof useCreateSupportTicket>;
import { SegmentedControl, SegmentOption } from "./SegmentedControl";
import {
  buildCreateTicketRequest,
  ENVIRONMENT_OPTIONS,
  SEVERITY_OPTIONS,
  TicketFormValues,
  TicketSeverity,
} from "./ticketForm";

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

const FieldLabel = ({ children }: { children: React.ReactNode }) => (
  <Text as="legend" textStyle="text-ui-med" fontWeight="600" mb="2">
    {children}
  </Text>
);

/** A severity choice styled as a card, backed by a native radio input. */
const SeverityCard = ({
  registration,
  value,
  label,
  description,
  isSelected,
}: {
  registration: UseFormRegisterReturn<"severity">;
  value: TicketSeverity;
  label: string;
  description: string;
  isSelected: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isUrgent = value === "urgent";
  const selectedBorder = isUrgent ? colors.accent.red : colors.accent.purple;
  return (
    <Box
      as="label"
      display="flex"
      flexDirection="column"
      gap="0.5"
      minHeight="44px"
      px="3"
      py="2.5"
      borderRadius="lg"
      cursor="pointer"
      borderWidth={isSelected ? "2px" : "1px"}
      borderColor={isSelected ? selectedBorder : colors.border.secondary}
      bg={
        isSelected
          ? isUrgent
            ? colors.background.error
            : colors.background.accent
          : colors.background.primary
      }
      _focusWithin={{ boxShadow: "input.focus" }}
    >
      <VisuallyHiddenInput type="radio" value={value} {...registration} />
      <Text textStyle="text-ui-med" fontWeight="600">
        {label}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {description}
      </Text>
    </Box>
  );
};

/** One button of a segmented control, backed by a native radio input. */
type TicketDetails = { label: string; value: React.ReactNode }[];

const NewTicketForm = ({
  organizationId,
  organizationName,
  defaultSeverity,
  submit,
  onClose,
}: {
  organizationId?: string;
  organizationName?: string;
  defaultSeverity: TicketSeverity;
  submit: CreateTicketMutation;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const context = useTicketContext();
  const { register, handleSubmit, formState, watch } =
    useForm<TicketFormValues>({
      mode: "onTouched",
      defaultValues: {
        title: "",
        severity: defaultSeverity,
        environmentType: "production",
        description: "",
        includeDetails: true,
      },
    });
  const { errors } = formState;
  const severity = watch("severity");
  const environmentType = watch("environmentType");
  const includeDetails = watch("includeDetails");
  const severityField = register("severity");
  const environmentField = register("environmentType");

  // The global API always adds the org ID (and the user's email), so the
  // organization is shown whether or not the optional details are included.
  const alwaysSent: TicketDetails = [];
  if (organizationName || organizationId) {
    alwaysSent.push({
      label: "Organization",
      value: (
        <>
          {organizationName}{" "}
          {organizationId && (
            <Text
              as="span"
              textStyle="monospace"
              fontSize="xs"
              color={colors.foreground.secondary}
            >
              {organizationId}
            </Text>
          )}
        </>
      ),
    });
  }
  const optional: TicketDetails = [];
  if (context.region) {
    optional.push({ label: "Environment", value: `${context.region} · Cloud` });
  }
  if (context.environmentVersion) {
    optional.push({
      label: "Version",
      value: (
        <Text as="span" textStyle="monospace" fontSize="xs">
          {context.environmentVersion}
        </Text>
      ),
    });
  }

  const isUrgent = severity === "urgent";

  const onSubmit = (values: TicketFormValues) =>
    submit.mutate(buildCreateTicketRequest(values, context));

  return (
    <Flex
      as="form"
      onSubmit={handleSubmit(onSubmit)}
      noValidate
      direction="column"
      height="100%"
    >
      <DrawerBody px="5" py="4">
        <VStack spacing="4" align="stretch">
          {submit.isError && (
            <Alert
              variant="error"
              minWidth="0"
              message={
                // Intercom already has a contact with this email that support
                // needs to link to the user; retrying won't help.
                submit.error instanceof OpenApiFetchError &&
                submit.error.status === 409
                  ? `Your support account needs to be linked before you can open tickets here. Email ${SUPPORT_EMAIL} and we'll set it up.`
                  : isUrgent
                    ? `Your ticket couldn't be opened. Try again, or email ${SUPPORT_EMAIL} with \u201curgent\u201d in the subject.`
                    : `Your ticket couldn't be opened. Try again, or email ${SUPPORT_EMAIL}.`
              }
            />
          )}

          <Box as="fieldset">
            <FieldLabel>Severity</FieldLabel>
            <Grid templateColumns="repeat(2, minmax(0, 1fr))" gap="2">
              {SEVERITY_OPTIONS.map((option) => (
                <SeverityCard
                  key={option.value}
                  registration={severityField}
                  value={option.value}
                  label={option.label}
                  description={option.description}
                  isSelected={severity === option.value}
                />
              ))}
            </Grid>
            {severity === "urgent" && (
              <Box
                mt="2"
                px="3"
                py="2.5"
                borderRadius="lg"
                borderWidth="1px"
                borderColor={colors.border.error}
                bg={colors.background.error}
                textStyle="text-small"
                data-testid="support-ticket-urgent-notice"
              >
                Urgent tickets are production outages.{" "}
                <Text as="span" fontWeight="600" color={colors.accent.red}>
                  On plans with an SLA, they page on-call
                </Text>{" "}
                right away.
              </Box>
            )}
          </Box>

          <Box as="fieldset">
            <FieldLabel>Environment</FieldLabel>
            <SegmentedControl>
              {ENVIRONMENT_OPTIONS.map((option) => (
                <SegmentOption
                  key={option.value}
                  inputProps={environmentField}
                  value={option.value}
                  label={option.label}
                  isSelected={environmentType === option.value}
                />
              ))}
            </SegmentedControl>
          </Box>

          <FormControl isRequired isInvalid={!!errors.title}>
            <FormLabel fontWeight="600">Subject</FormLabel>
            <Input
              {...register("title", {
                validate: (value) =>
                  value.trim().length > 0 || "A subject is required.",
                maxLength: {
                  value: 200,
                  message: "Keep the subject under 200 characters.",
                },
              })}
              placeholder="e.g. prod_ingest replica keeps restarting"
              variant={errors.title ? "error" : "default"}
            />
            <FormErrorMessage>{errors.title?.message}</FormErrorMessage>
          </FormControl>

          <FormControl isRequired isInvalid={!!errors.description}>
            <FormLabel fontWeight="600">What happened?</FormLabel>
            <Textarea
              {...register("description", {
                validate: (value) =>
                  value.trim().length > 0 || "Describe what happened.",
                maxLength: {
                  value: 10000,
                  message: "Keep the description under 10,000 characters.",
                },
              })}
              rows={4}
              resize="vertical"
              placeholder="What you expected, what happened instead, and the cluster or object involved."
            />
            <FormErrorMessage>{errors.description?.message}</FormErrorMessage>
          </FormControl>

          {(alwaysSent.length > 0 || optional.length > 0) && (
            <Box
              as="section"
              aria-label="Sent with your ticket"
              borderWidth="1px"
              borderColor={colors.border.primary}
              borderRadius="lg"
              bg={colors.background.secondary}
            >
              {optional.length > 0 ? (
                <HStack spacing="2.5" px="3" py="2.5">
                  <Checkbox {...register("includeDetails")} flex="1">
                    <Text textStyle="text-ui-med" fontWeight="600">
                      Include environment details
                    </Text>
                  </Checkbox>
                  <Text
                    textStyle="text-small"
                    color={colors.foreground.secondary}
                  >
                    Saves a round trip
                  </Text>
                </HStack>
              ) : (
                <Text textStyle="text-ui-med" fontWeight="600" px="3" py="2.5">
                  Sent with your ticket
                </Text>
              )}
              <Grid
                as="dl"
                templateColumns="132px minmax(0, 1fr)"
                columnGap="3"
                rowGap="1.5"
                pl={optional.length > 0 ? "38px" : "3"}
                pr="3"
                pb="3"
                textStyle="text-small"
                data-testid="support-ticket-context"
              >
                {[...alwaysSent, ...(includeDetails ? optional : [])].map(
                  (detail) => (
                    <React.Fragment key={detail.label}>
                      <Text as="dt" color={colors.foreground.secondary}>
                        {detail.label}
                      </Text>
                      <Text as="dd" minWidth="0" overflowWrap="anywhere">
                        {detail.value}
                      </Text>
                    </React.Fragment>
                  ),
                )}
              </Grid>
            </Box>
          )}
        </VStack>
      </DrawerBody>
      <DrawerFooter
        gap="2"
        px="5"
        py="3"
        borderTopWidth="1px"
        borderColor={colors.border.primary}
      >
        <Button variant="secondary" onClick={onClose}>
          Cancel
        </Button>
        <Button type="submit" variant="primary" isLoading={submit.isPending}>
          {isUrgent ? "Report outage" : "Open ticket"}
        </Button>
      </DrawerFooter>
    </Flex>
  );
};

const severityLabel = (severity: TicketSeverity) =>
  SEVERITY_OPTIONS.find((option) => option.value === severity)?.label ??
  severity;

const ENVIRONMENT_LABELS = Object.fromEntries(
  ENVIRONMENT_OPTIONS.map((option) => [option.value, option.label]),
);

/** The confirmation, built from the request that was sent. */
const TicketSent = ({
  ticket,
  request,
  onReportOutage,
  onClose,
}: {
  ticket: CreatedSupportTicket;
  request: CreateSupportTicketRequest;
  onReportOutage: () => void;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  // The request has the region only when it was included.
  const tags = [
    severityLabel(request.severity),
    ENVIRONMENT_LABELS[request.environmentType],
    request.region,
  ].filter(Boolean);

  return (
    <DrawerBody px="6" py="7" data-testid="support-ticket-sent">
      <VStack spacing="5" align="stretch">
        <VStack spacing="3" align="start">
          <Flex
            boxSize="10"
            borderRadius="full"
            bg={colors.background.tertiary}
            align="center"
            justify="center"
          >
            {/* Chakra icons take their color as a prop. The SVG's own
                width and height read as theme sizes, so set the size too. */}
            <CheckmarkIcon
              aria-hidden
              color={colors.accent.green}
              boxSize="5"
            />
          </Flex>
          <Text as="h3" textStyle="heading-md">
            {ticket.number
              ? `Ticket #${ticket.number} is open`
              : "Your ticket is open"}
          </Text>
          <Text color={colors.foreground.secondary}>{request.title}</Text>
          <HStack spacing="1.5" flexWrap="wrap">
            {tags.map((tag, index) => (
              <Tag
                key={tag}
                size="sm"
                borderRadius="md"
                fontWeight="500"
                bg={
                  index === 0
                    ? colors.background.accent
                    : colors.background.tertiary
                }
                color={
                  index === 0 ? colors.accent.purple : colors.foreground.primary
                }
              >
                {tag}
              </Tag>
            ))}
          </HStack>
        </VStack>

        <VStack
          as="section"
          aria-label="What happens next"
          spacing="2.5"
          align="stretch"
        >
          <Text as="h4" textStyle="text-ui-med" fontWeight="600">
            What happens next
          </Text>
          <OrderedList spacing="2" pl="1">
            <ListItem>
              {ticket.pagesOnCall
                ? "On-call has been paged."
                : `A support engineer picks it up within the ${severityLabel(
                    request.severity,
                  )} response target in your support plan.`}
            </ListItem>
            <ListItem>Replies arrive by email and in Help Center.</ListItem>
          </OrderedList>
        </VStack>

        <HStack spacing="2" flexWrap="wrap">
          <Button
            as="a"
            href={HELP_CENTER_TICKETS_URL}
            {...EXTERNAL_LINK_PROPS}
            variant="primary"
          >
            View in Help Center ↗
          </Button>
          <Button variant="secondary" onClick={onClose}>
            Done
          </Button>
        </HStack>

        {request.severity !== "urgent" && (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            pt="4"
            borderTopWidth="1px"
            borderColor={colors.border.primary}
          >
            Production went down?{" "}
            <Button
              variant="link"
              onClick={onReportOutage}
              fontWeight="500"
              fontSize="inherit"
              color={colors.accent.red}
            >
              Report a production outage
            </Button>{" "}
            instead.
          </Text>
        )}
      </VStack>
    </DrawerBody>
  );
};

export interface NewSupportTicketDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  /** The severity the form starts on, e.g. urgent for an outage report. */
  defaultSeverity?: TicketSeverity;
  /** Starts a new ticket as an outage report. */
  onReportOutage: () => void;
  organizationId?: string;
  organizationName?: string;
}

export const NewSupportTicketDrawer = ({
  isOpen,
  onClose,
  defaultSeverity = "normal",
  onReportOutage,
  organizationId,
  organizationName,
}: NewSupportTicketDrawerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const region = useAtomValue(currentRegionIdAtom) ?? undefined;
  // The Messenger launcher floats over the drawer's footer buttons.
  useHideIntercomLauncher(isOpen);
  const submit = useCreateSupportTicket();
  const sent =
    submit.isSuccess && submit.variables
      ? { ticket: submit.data, request: submit.variables }
      : null;

  const scope = [organizationName, region].filter(Boolean).join(" · ");

  return (
    <Drawer
      isOpen={isOpen}
      onClose={onClose}
      // Start over the next time it opens.
      onCloseComplete={() => submit.reset()}
      placement="right"
      size="md"
    >
      <DrawerOverlay />
      <DrawerContent maxWidth="560px">
        <DrawerHeader
          display="flex"
          alignItems="center"
          gap="2"
          px="5"
          py="4"
          borderBottomWidth="1px"
          borderColor={colors.border.primary}
        >
          <IconButton
            aria-label={sent ? "Close" : "Back to help and support"}
            icon={<ChevronLeftIcon />}
            variant="ghost"
            size="sm"
            onClick={onClose}
          />
          <Text as="h2" textStyle="heading-sm" flex="1">
            {sent ? "Help and support" : "Open a ticket"}
          </Text>
          {!sent && scope && (
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              noOfLines={1}
            >
              {scope}
            </Text>
          )}
        </DrawerHeader>
        {isOpen &&
          (sent ? (
            <TicketSent
              ticket={sent.ticket}
              request={sent.request}
              onReportOutage={() => {
                submit.reset();
                onReportOutage();
              }}
              onClose={onClose}
            />
          ) : (
            <NewTicketForm
              // Start over when the starting severity changes.
              key={defaultSeverity}
              organizationId={organizationId}
              organizationName={organizationName}
              defaultSeverity={defaultSeverity}
              submit={submit}
              onClose={onClose}
            />
          ))}
      </DrawerContent>
    </Drawer>
  );
};
