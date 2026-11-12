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
import SimpleSelect from "~/components/SimpleSelect";
import TextLink from "~/components/TextLink";
import {
  HELP_CENTER_TICKETS_URL,
  MZ_DEBUG_SELF_MANAGED_URL,
  SUPPORT_EMAIL,
} from "~/externalUrls";
import { useHideIntercomLauncher } from "~/hooks/useIntercom";
import { CheckmarkIcon, ChevronLeftIcon } from "~/icons";
import {
  currentEnvironmentState,
  currentRegionIdAtom,
} from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { AttachFilesStatus } from "./AttachFilesStatus";
import { AttachmentPicker, PickedFile } from "./AttachmentPicker";
import { EXTERNAL_LINK_PROPS } from "./externalLink";
import { useCreateSupportTicket, useSupportOrganizations } from "./queries";
import { SegmentedControl, SegmentOption } from "./SegmentedControl";
import {
  attachmentError,
  buildCreateTicketRequest,
  buildTicketTargets,
  defaultTicketTarget,
  ENVIRONMENT_OPTIONS,
  OUTAGE_TEMPLATE,
  SEVERITY_OPTIONS,
  TICKET_SEVERITY_OPTIONS,
  TICKET_TEMPLATE,
  TicketFormValues,
  TicketSeverity,
} from "./ticketForm";

type CreateTicketMutation = ReturnType<typeof useCreateSupportTicket>;

/**
 * How to share files after the ticket is open: more than the form takes, or
 * if adding them failed. Intercom limits incoming email to 20 MB in total.
 */
const FilesNote = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text
      textStyle="text-small"
      color={colors.foreground.secondary}
      data-testid="support-ticket-files-note"
    >
      <Text as="span" fontWeight="600" color={colors.foreground.primary}>
        Need to share more files?
      </Text>{" "}
      Reply to the confirmation email (up to 20 MB in total) or open the ticket
      in the{" "}
      <TextLink href={HELP_CENTER_TICKETS_URL} isExternal>
        Help Center ↗
      </TextLink>
      . Please make sure they don&apos;t include passwords, credentials, or
      other sensitive data.
    </Text>
  );
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
  isOutage,
  submit,
  onReportOutage,
  onClose,
}: {
  organizationId?: string;
  organizationName?: string;
  /** The outage form: urgent and production only. */
  isOutage: boolean;
  submit: CreateTicketMutation;
  onReportOutage: () => void;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [files, setFiles] = React.useState<PickedFile[]>([]);
  const filesError = attachmentError(files.map(({ file }) => file));
  const signedIn = { ...useTicketContext(), organizationId, organizationName };
  const supportOrganizations = useSupportOrganizations();
  // One choice per organization and active region.
  const targets = buildTicketTargets(supportOrganizations.data ?? [], signedIn);
  const descriptionPlaceholder = isOutage ? OUTAGE_TEMPLATE : TICKET_TEMPLATE;
  const { register, handleSubmit, formState, watch } =
    useForm<TicketFormValues>({
      mode: "onTouched",
      defaultValues: {
        title: "",
        severity: isOutage ? "urgent" : "normal",
        environmentType: "production",
        description: "",
        target: defaultTicketTarget(targets, signedIn)?.key ?? "",
      },
    });
  const { errors } = formState;
  const severity = watch("severity");
  const environmentType = watch("environmentType");
  const targetKey = watch("target");
  const severityField = register("severity");
  const environmentField = register("environmentType");
  // The list can arrive after the form opens; until then the choice is the
  // signed-in organization.
  const target =
    targets.find(({ key }) => key === targetKey) ??
    defaultTicketTarget(targets, signedIn);

  // The global API always adds the org ID (and the user's email).
  const sentDetails: TicketDetails = [];
  if (target) {
    sentDetails.push({
      label: "Organization",
      value: (
        <>
          {target.organizationName}{" "}
          <Text
            as="span"
            textStyle="monospace"
            fontSize="xs"
            color={colors.foreground.secondary}
          >
            {target.organizationId}
          </Text>
        </>
      ),
    });
    if (target.region) {
      sentDetails.push({ label: "Region", value: target.region });
    }
    if (target.environmentVersion) {
      sentDetails.push({
        label: "Version",
        value: (
          <Text as="span" textStyle="monospace" fontSize="xs">
            {target.environmentVersion}
          </Text>
        ),
      });
    }
  }

  const onSubmit = (values: TicketFormValues) => {
    // The picker shows why; the ticket waits until the files can go with it.
    if (filesError) return;
    // The files are added once the ticket is open, even if the drawer has
    // closed by then.
    submit.mutate({
      request: buildCreateTicketRequest(
        // Outages are always about production.
        isOutage
          ? { ...values, severity: "urgent", environmentType: "production" }
          : values,
        target,
      ),
      files: files.map(({ file }) => file),
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
              message={
                // Intercom already has a contact with this email that support
                // needs to link to the user; retrying won't help.
                submit.error instanceof OpenApiFetchError &&
                submit.error.status === 409
                  ? `Your support account needs to be linked before you can open tickets here. Email ${SUPPORT_EMAIL} and we'll set it up.`
                  : isOutage
                    ? `Your outage report couldn't be sent. Try again, or email ${SUPPORT_EMAIL} with \u201curgent\u201d in the subject.`
                    : `Your ticket couldn't be opened. Try again, or email ${SUPPORT_EMAIL}.`
              }
            />
          )}

          {isOutage ? (
            <Box
              px="3"
              py="2.5"
              borderRadius="lg"
              borderWidth="1px"
              borderColor={colors.border.error}
              bg={colors.background.error}
              textStyle="text-small"
              data-testid="support-ticket-urgent-notice"
            >
              <Text as="span" fontWeight="600" color={colors.accent.red}>
                Plans with an SLA will page on-call
              </Text>{" "}
              right away.
            </Box>
          ) : (
            <Box
              px="3"
              py="2.5"
              borderRadius="lg"
              borderWidth="1px"
              borderColor={colors.border.secondary}
              bg={colors.background.secondary}
              textStyle="text-small"
              data-testid="support-ticket-outage-callout"
            >
              Production down? Use{" "}
              <Button
                variant="link"
                onClick={onReportOutage}
                fontWeight="600"
                fontSize="inherit"
                color={colors.accent.red}
              >
                Report a production outage
              </Button>{" "}
              instead.
            </Box>
          )}

          {targets.length > 1 && (
            <FormControl>
              <FormLabel fontWeight="600">Organization and region</FormLabel>
              <SimpleSelect
                {...register("target")}
                // Shows the choice the ticket will use, also while the saved
                // one isn't offered (e.g. before the current region is known).
                value={target?.key}
                width="100%"
              >
                {targets.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.region
                      ? `${option.organizationName} \u00b7 ${option.region}`
                      : option.organizationName}
                  </option>
                ))}
              </SimpleSelect>
            </FormControl>
          )}

          {!isOutage && (
            <Box as="fieldset">
              <FieldLabel>Severity</FieldLabel>
              <Grid templateColumns="repeat(3, minmax(0, 1fr))" gap="2">
                {TICKET_SEVERITY_OPTIONS.map((option) => (
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
            </Box>
          )}

          <Box as="fieldset">
            <FieldLabel>Environment</FieldLabel>
            {isOutage ? (
              // Anyone reporting an outage is reporting it in production.
              <Text
                textStyle="text-ui-reg"
                data-testid="support-ticket-environment"
              >
                Production
              </Text>
            ) : (
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
            )}
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
              placeholder={descriptionPlaceholder}
              variant={errors.description ? "error" : "default"}
              rows={6}
              minH="9.5rem"
              lineHeight="1.4"
              resize="vertical"
            />
            <FormErrorMessage>{errors.description?.message}</FormErrorMessage>
          </FormControl>

          {isOutage && (
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              data-testid="support-ticket-mz-debug"
            >
              <Text as="em">Running Materialize self-managed?</Text> If you can,
              prepare an{" "}
              <Text as="span" textStyle="monospace" fontSize="xs">
                mz-debug
              </Text>{" "}
              while you wait for a response. Our team may ask for it to help
              diagnose the issue.{" "}
              <TextLink href={MZ_DEBUG_SELF_MANAGED_URL} isExternal>
                How to run mz-debug ↗
              </TextLink>
            </Text>
          )}

          <AttachmentPicker
            files={files}
            error={filesError}
            isDisabled={submit.isPending}
            onChange={setFiles}
          />

          {sentDetails.length > 0 && (
            <Box
              as="section"
              aria-label="Sent with your ticket"
              borderWidth="1px"
              borderColor={colors.border.primary}
              borderRadius="lg"
              bg={colors.background.secondary}
            >
              <Text textStyle="text-ui-med" fontWeight="600" px="3" py="2.5">
                Sent with your ticket
              </Text>
              <Grid
                as="dl"
                templateColumns="132px minmax(0, 1fr)"
                columnGap="3"
                rowGap="1.5"
                px="3"
                pb="3"
                textStyle="text-small"
                data-testid="support-ticket-context"
              >
                {sentDetails.map((detail) => (
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
          {isOutage ? "Report outage" : "Open ticket"}
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
  signedInOrganizationId,
  onReportOutage,
  onClose,
}: {
  ticket: CreatedSupportTicket;
  request: CreateSupportTicketRequest;
  signedInOrganizationId?: string;
  onReportOutage: () => void;
  onClose: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const supportOrganizations = useSupportOrganizations();
  // A ticket for another organization says which one it went to.
  const otherOrganizationId =
    request.organizationId && request.organizationId !== signedInOrganizationId
      ? request.organizationId
      : undefined;
  const otherOrganization =
    otherOrganizationId &&
    (supportOrganizations.data?.find(({ id }) => id === otherOrganizationId)
      ?.name ??
      otherOrganizationId.slice(0, 8));
  // The request has the region only when it was included.
  const tags = [
    severityLabel(request.severity),
    ENVIRONMENT_LABELS[request.environmentType],
    otherOrganization,
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
          <AttachFilesStatus ticketId={ticket.id} />
          <FilesNote />
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
  /** Shows the outage form instead of the default one. */
  isOutage?: boolean;
  /** Starts a new ticket as an outage report. */
  onReportOutage: () => void;
  organizationId?: string;
  organizationName?: string;
}

export const NewSupportTicketDrawer = ({
  isOpen,
  onClose,
  isOutage = false,
  onReportOutage,
  organizationId,
  organizationName,
}: NewSupportTicketDrawerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  // The Messenger launcher floats over the drawer's footer buttons.
  useHideIntercomLauncher(isOpen);
  const submit = useCreateSupportTicket();
  const startOver = () => submit.reset();
  const sent =
    submit.isSuccess && submit.variables
      ? { ticket: submit.data, request: submit.variables.request }
      : null;

  return (
    <Drawer
      isOpen={isOpen}
      onClose={onClose}
      // Start over the next time it opens.
      onCloseComplete={startOver}
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
            {sent
              ? "Help and support"
              : isOutage
                ? "Report a production outage"
                : "Open a ticket"}
          </Text>
        </DrawerHeader>
        {isOpen &&
          (sent ? (
            <TicketSent
              ticket={sent.ticket}
              request={sent.request}
              signedInOrganizationId={organizationId}
              onReportOutage={() => {
                startOver();
                onReportOutage();
              }}
              onClose={onClose}
            />
          ) : (
            <NewTicketForm
              // Start over when switching between the forms.
              key={isOutage ? "outage" : "ticket"}
              organizationId={organizationId}
              organizationName={organizationName}
              isOutage={isOutage}
              submit={submit}
              onReportOutage={() => {
                startOver();
                onReportOutage();
              }}
              onClose={onClose}
            />
          ))}
      </DrawerContent>
    </Drawer>
  );
};
