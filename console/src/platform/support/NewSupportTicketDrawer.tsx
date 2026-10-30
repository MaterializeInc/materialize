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
import { useMutation } from "@tanstack/react-query";
import { useAtomValue } from "jotai";
import React from "react";
import { useForm, UseFormRegisterReturn } from "react-hook-form";

import { Alert } from "~/components/Alert";
import SimpleSelect from "~/components/SimpleSelect";
import { HELP_CENTER_TICKETS_URL, SUPPORT_EMAIL } from "~/externalUrls";
import { useHideIntercomLauncher } from "~/hooks/useIntercom";
import { CheckmarkIcon, ChevronLeftIcon } from "~/icons";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { submitTicket, SubmitTicketResult } from "./submitTicket";
import {
  buildTicketPayload,
  ENVIRONMENT_OPTIONS,
  PLATFORM_OPTIONS,
  SEVERITY_OPTIONS,
  TicketFormValues,
  TicketPlatform,
  TicketSeverity,
} from "./ticketForm";

const EXTERNAL_LINK_PROPS = {
  target: "_blank",
  rel: "noopener noreferrer",
} as const;

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

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
      minHeight="11"
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
            : colors.purple[50]
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
const SegmentOption = ({
  registration,
  value,
  label,
  isSelected,
  isLast,
}: {
  registration: UseFormRegisterReturn<"environmentType">;
  value: string;
  label: string;
  isSelected: boolean;
  isLast: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      as="label"
      display="inline-flex"
      alignItems="center"
      minHeight="9"
      px="3.5"
      cursor="pointer"
      fontWeight="500"
      textStyle="text-ui-med"
      borderRightWidth={isLast ? "0" : "1px"}
      borderColor={colors.border.secondary}
      bg={isSelected ? colors.accent.purple : colors.background.primary}
      color={isSelected ? colors.foreground.inverse : colors.foreground.primary}
      _focusWithin={{ boxShadow: "input.focus" }}
    >
      <VisuallyHiddenInput type="radio" value={value} {...registration} />
      {label}
    </Box>
  );
};

type TicketDetails = { label: string; value: React.ReactNode }[];

const NewTicketForm = ({
  organizationId,
  organizationName,
  isAnonymous,
  defaultSeverity,
  defaultPlatform,
  onSubmitted,
  onClose,
}: {
  organizationId?: string;
  organizationName?: string;
  isAnonymous: boolean;
  defaultSeverity: TicketSeverity;
  defaultPlatform: TicketPlatform;
  onSubmitted: (result: SubmitTicketResult, values: TicketFormValues) => void;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const context = useTicketContext();
  const submit = useMutation({ mutationFn: submitTicket });
  const { register, handleSubmit, formState, watch } =
    useForm<TicketFormValues>({
      mode: "onTouched",
      defaultValues: {
        title: "",
        severity: defaultSeverity,
        environmentType: "production",
        description: "",
        email: "",
        name: "",
        companyName: "",
        platform: defaultPlatform,
        includeDetails: true,
      },
    });
  const { errors } = formState;
  const severity = watch("severity");
  const environmentType = watch("environmentType");
  const includeDetails = watch("includeDetails");
  const severityField = register("severity");
  const environmentField = register("environmentType");

  const details: TicketDetails = [];
  if (organizationName || organizationId) {
    details.push({
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
  if (context.region) {
    details.push({ label: "Environment", value: `${context.region} · Cloud` });
  }
  if (context.environmentVersion) {
    details.push({
      label: "Version",
      value: (
        <Text as="span" textStyle="monospace" fontSize="xs">
          {context.environmentVersion}
        </Text>
      ),
    });
  }

  const onSubmit = (values: TicketFormValues) => {
    const payload = buildTicketPayload(values, {
      ...(values.includeDetails ? context : {}),
      organizationId: values.includeDetails ? organizationId : undefined,
      isAnonymous,
    });
    submit.mutate(payload, {
      onSuccess: (result) => onSubmitted(result, values),
    });
  };

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
              message={`Your ticket couldn't be opened. Try again, or email ${SUPPORT_EMAIL}.`}
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
                Production down?{" "}
                <Text as="span" fontWeight="600" color={colors.accent.red}>
                  Urgent tickets page on-call
                </Text>{" "}
                right away. Use it only when there&apos;s no workaround.
              </Box>
            )}
          </Box>

          <Box as="fieldset">
            <FieldLabel>Environment</FieldLabel>
            <Flex
              width="fit-content"
              borderWidth="1px"
              borderColor={colors.border.secondary}
              borderRadius="lg"
              overflow="hidden"
            >
              {ENVIRONMENT_OPTIONS.map((option, index) => (
                <SegmentOption
                  key={option.value}
                  registration={environmentField}
                  value={option.value}
                  label={option.label}
                  isSelected={environmentType === option.value}
                  isLast={index === ENVIRONMENT_OPTIONS.length - 1}
                />
              ))}
            </Flex>
          </Box>

          {isAnonymous && (
            <Grid templateColumns="repeat(2, minmax(0, 1fr))" gap="4">
              <FormControl isRequired isInvalid={!!errors.email}>
                <FormLabel fontWeight="600">Email</FormLabel>
                <Input
                  {...register("email", {
                    validate: (value) =>
                      value.trim().length === 0
                        ? "An email is required, so we can reply."
                        : EMAIL_PATTERN.test(value.trim()) ||
                          "Enter a valid email address.",
                  })}
                  type="email"
                  autoComplete="email"
                  variant={errors.email ? "error" : "default"}
                />
                <FormErrorMessage>{errors.email?.message}</FormErrorMessage>
              </FormControl>
              <FormControl>
                <FormLabel fontWeight="600">Name</FormLabel>
                <Input {...register("name")} autoComplete="name" />
              </FormControl>
              <FormControl isRequired isInvalid={!!errors.companyName}>
                <FormLabel fontWeight="600">Company</FormLabel>
                <Input
                  {...register("companyName", {
                    validate: (value) =>
                      value.trim().length > 0 || "A company is required.",
                  })}
                  autoComplete="organization"
                  variant={errors.companyName ? "error" : "default"}
                />
                <FormErrorMessage>
                  {errors.companyName?.message}
                </FormErrorMessage>
              </FormControl>
              <FormControl isRequired>
                <FormLabel fontWeight="600">Platform</FormLabel>
                <SimpleSelect {...register("platform")}>
                  {PLATFORM_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </SimpleSelect>
              </FormControl>
            </Grid>
          )}

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

          {details.length > 0 && (
            <Box
              as="section"
              aria-label="Sent with your ticket"
              borderWidth="1px"
              borderColor={colors.border.primary}
              borderRadius="lg"
              bg={colors.background.secondary}
            >
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
              {includeDetails && (
                <Grid
                  as="dl"
                  templateColumns="132px minmax(0, 1fr)"
                  columnGap="3"
                  rowGap="1.5"
                  pl="9.5"
                  pr="3"
                  pb="3"
                  textStyle="text-small"
                  data-testid="support-ticket-context"
                >
                  {details.map((detail) => (
                    <React.Fragment key={detail.label}>
                      <Text as="dt" color={colors.foreground.secondary}>
                        {detail.label}
                      </Text>
                      <Text as="dd" minWidth="0" overflowWrap="anywhere">
                        {detail.value}
                      </Text>
                    </React.Fragment>
                  ))}
                </Grid>
              )}
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
          Open ticket
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

const TicketSent = ({
  result,
  values,
  organizationName,
  region,
  onReportOutage,
  onClose,
}: {
  result: SubmitTicketResult;
  values: TicketFormValues;
  organizationName?: string;
  region?: string;
  onReportOutage: () => void;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isUrgent = values.severity === "urgent";
  const tags = [
    severityLabel(values.severity),
    ENVIRONMENT_LABELS[values.environmentType],
    region,
  ].filter(Boolean);

  return (
    <DrawerBody px="6" py="7" data-testid="support-ticket-sent">
      <VStack spacing="5" align="stretch">
        {!result.sent && (
          <Alert
            variant="info"
            minWidth="0"
            message="Preview only: ticket submission isn't connected yet, so nothing was sent."
          />
        )}
        <VStack spacing="3" align="start">
          <Flex
            boxSize="10"
            borderRadius="full"
            bg={colors.green[50]}
            align="center"
            justify="center"
            color={colors.green[600]}
          >
            <CheckmarkIcon aria-hidden />
          </Flex>
          <Text as="h3" textStyle="heading-md">
            {result.ticketNumber
              ? `Ticket #${result.ticketNumber} is open`
              : "Your ticket is open"}
          </Text>
          <Text color={colors.foreground.secondary}>{values.title.trim()}</Text>
          <HStack spacing="1.5" flexWrap="wrap">
            {tags.map((tag, index) => (
              <Tag
                key={tag}
                size="sm"
                borderRadius="md"
                fontWeight="500"
                bg={
                  index === 0
                    ? isUrgent
                      ? colors.background.error
                      : colors.purple[50]
                    : colors.background.tertiary
                }
                color={
                  index === 0
                    ? isUrgent
                      ? colors.accent.red
                      : colors.purple[600]
                    : colors.foreground.primary
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
              {isUrgent
                ? "On-call is paged right away."
                : `A support engineer picks it up within the ${severityLabel(values.severity)} response target in your support plan.`}
            </ListItem>
            <ListItem>Replies arrive by email and in Help Center.</ListItem>
            {organizationName && (
              <ListItem>
                Admins in {organizationName} can see and reply to this ticket
                too.
              </ListItem>
            )}
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

        {!isUrgent && (
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
            to page on-call.
          </Text>
        )}
      </VStack>
    </DrawerBody>
  );
};

export interface NewSupportTicketDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  /** Reopens the form with Urgent selected. */
  onReportOutage: () => void;
  organizationId?: string;
  organizationName?: string;
  /** True when the Messenger doesn't know who the user is. */
  isAnonymous: boolean;
  defaultSeverity?: TicketSeverity;
  defaultPlatform?: TicketPlatform;
}

export const NewSupportTicketDrawer = ({
  isOpen,
  onClose,
  onReportOutage,
  organizationId,
  organizationName,
  isAnonymous,
  defaultSeverity = "normal",
  defaultPlatform = "Materialize Cloud",
}: NewSupportTicketDrawerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const region = useAtomValue(currentRegionIdAtom) ?? undefined;
  // The Messenger launcher floats over the drawer's footer buttons.
  useHideIntercomLauncher(isOpen);
  const [sent, setSent] = React.useState<{
    result: SubmitTicketResult;
    values: TicketFormValues;
  } | null>(null);

  // Start over each time the drawer opens.
  React.useEffect(() => {
    if (isOpen) setSent(null);
  }, [isOpen, defaultSeverity]);

  const scope = [organizationName, region].filter(Boolean).join(" · ");

  return (
    <Drawer isOpen={isOpen} onClose={onClose} placement="right" size="md">
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
              result={sent.result}
              values={sent.values}
              organizationName={organizationName}
              region={region}
              onReportOutage={onReportOutage}
              onClose={onClose}
            />
          ) : (
            <NewTicketForm
              key={defaultSeverity}
              organizationId={organizationId}
              organizationName={organizationName}
              isAnonymous={isAnonymous}
              defaultSeverity={defaultSeverity}
              defaultPlatform={defaultPlatform}
              onSubmitted={(result, values) => setSent({ result, values })}
              onClose={onClose}
            />
          ))}
      </DrawerContent>
    </Drawer>
  );
};
