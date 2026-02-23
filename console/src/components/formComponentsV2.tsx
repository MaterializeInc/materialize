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
  BoxProps,
  Button,
  ButtonProps,
  Flex,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  FormLabelProps,
  Grid,
  Hide,
  HStack,
  Link as ChakraLink,
  ModalFooter,
  ModalHeader,
  Show,
  Stack,
  StackDivider,
  StackProps,
  Step,
  StepIndicator,
  Stepper,
  StepSeparator,
  StepTitle,
  Text,
  Tooltip,
  useModalContext,
  UseStepsReturn,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { PropsWithChildren } from "react";
import { Link } from "react-router-dom";

import { InfoIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

export const FORM_COLUMN_GAP = 88;
export const FORM_CONTENT_WIDTH = 552;
export const FORM_LABEL_LINE_HEIGHT = 6;
export const FORM_CONTROL_WIDTH = 320;

export type WizardStep = {
  id: string;
  label: string;
};

export type FormHeaderProps = {
  title: string;
  steps: WizardStep[];
} & UseStepsReturn;

export type FormFooterProps = {
  advanceType?: ButtonProps["type"];
  steps: WizardStep[];
  isValid: boolean;
  isPending?: boolean;
  submitMessage: string;
} & UseStepsReturn;

export const FormTopBar = ({ title, steps, activeStep }: FormHeaderProps) => {
  const { onClose } = useModalContext();
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <ModalHeader
      borderBottom="1px solid"
      borderBottomColor={colors.border.primary}
      px="4"
      py="3"
    >
      <Flex
        alignItems="center"
        justifyContent="center"
        direction={{ base: "column", sm: "row" }}
        gap="4"
      >
        <Box flex="1" flexBasis="0" textStyle="text-ui-med">
          {title}
        </Box>
        <Box textAlign="center" flexGrow="0" flexShrink="0" flexBasis="auto">
          <Hide below="md">
            {steps.length > 1 && (
              <Stepper index={activeStep} variant="modal-header">
                {steps.map((step) => (
                  <Step key={step.id}>
                    <StepIndicator />
                    <StepTitle>{step.label}</StepTitle>
                    <StepSeparator />
                  </Step>
                ))}
              </Stepper>
            )}
          </Hide>
          <Show below="md">
            <Box textStyle="text-ui-med">{steps[activeStep].label}</Box>
          </Show>
        </Box>
        <Box flex="1" flexBasis="0" textAlign="right">
          <Button onClick={onClose} variant="outline" size="sm">
            Exit
          </Button>
        </Box>
      </Flex>
    </ModalHeader>
  );
};

export const FormBottomBar = ({
  steps,
  activeStep,
  goToPrevious,
  goToNext,
  isValid,
  isPending,
  advanceType,
  submitMessage,
}: FormFooterProps) => {
  const { onClose } = useModalContext();
  const hasMoreSteps = steps.length - 1 > activeStep;

  const onBack = () => {
    if (activeStep === 0) {
      return onClose();
    }
    return goToPrevious();
  };

  const handleNextClick = () => {
    if (advanceType === "submit") {
      return false;
    }
    goToNext();
  };

  return (
    <ModalFooter justifyContent="space-between" px="4" py="3">
      <Button variant="outline" size="sm" onClick={onBack}>
        Back
      </Button>
      <Button
        variant="primary"
        size="sm"
        onClick={handleNextClick}
        type={advanceType ?? "button"}
        isLoading={isPending}
        isDisabled={!isValid}
      >
        {hasMoreSteps ? "Continue" : submitMessage} &rarr;
      </Button>
    </ModalFooter>
  );
};

export const FormAsideList = ({ children }: PropsWithChildren) => {
  return (
    <Stack
      direction={{ base: "row", lg: "column" }}
      alignItems="flex-start"
      divider={<StackDivider display={{ base: "none", lg: "block" }} />}
      gap={4}
      maxWidth={{ base: "100%", lg: 80 }}
      flexWrap="wrap"
      justifyContent="flex-start"
    >
      {children}
    </Stack>
  );
};

export const FormAsideItem = ({
  children,
  title,
  href,
  target,
}: PropsWithChildren<{ title: string; href?: string; target?: string }>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box maxWidth={{ base: "100%", md: "248px", lg: "100%" }}>
      <Box mb={1}>
        <ChakraLink
          as={Link}
          textStyle="text-small-heavy"
          color={colors.foreground.tertiary}
          to={href}
          target={target}
        >
          {title}
        </ChakraLink>
      </Box>
      <Box textStyle="text-small" mb={6}>
        {children}
      </Box>
    </Box>
  );
};

type FormSectionProps = {
  title: string;
  caption: string;
  description?: React.ReactNode;
};

export const FormSection = ({
  title,
  caption,
  description,
  children,
  ...stackProps
}: PropsWithChildren<FormSectionProps & StackProps>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack gap={6} mb={4} alignItems="flex-start" width="100%" {...stackProps}>
      <Box>
        <Text
          textStyle="text-small-heavy"
          lineHeight="20px"
          color={colors.foreground.secondary}
        >
          {caption}
        </Text>
        <Text as="h2" textStyle="heading-xs">
          {title}
        </Text>
        {description && <Text>{description}</Text>}
      </Box>
      {children}
    </VStack>
  );
};

export interface LabeledInputProps {
  error?: string;
  inputBoxProps?: BoxProps;
  label: string;
  labelProps?: FormLabelProps;
  message?: React.ReactNode;
  variant?: "default" | "stretch";

  tooltipMessage?: string | React.ReactNode;
}

export const LabeledInput = ({
  children,
  error,
  label,
  labelProps,
  inputBoxProps,
  message,
  tooltipMessage,
  variant = "default",
}: PropsWithChildren<LabeledInputProps>) => {
  return (
    <>
      <HStack spacing={0}>
        <FormLabel mb={0} {...labelProps}>
          {label}
        </FormLabel>
        {tooltipMessage && (
          <Tooltip label={tooltipMessage}>
            <InfoIcon ml="-2" />
          </Tooltip>
        )}
      </HStack>
      {message && <FormHelperText mt={0}>{message}</FormHelperText>}
      <Box
        maxWidth={variant === "stretch" ? "100%" : `${FORM_CONTROL_WIDTH}px`}
        mt={4}
        position="relative"
        {...inputBoxProps}
      >
        {children}
      </Box>
      {error && <FormErrorMessage>{error}</FormErrorMessage>}
    </>
  );
};

export interface FormContainerProps {
  aside?: React.ReactElement;
}

export const FormContainer = ({
  children,
  aside,
}: React.PropsWithChildren<FormContainerProps>) => {
  return (
    <Box mt={10}>
      <Grid
        templateColumns={{
          lg: `1fr ${FORM_CONTENT_WIDTH}px 3fr`,
          base: "1fr",
        }}
        templateRows={{ md: "auto 1fr", base: "auto auto 1fr " }}
        columnGap={`${FORM_COLUMN_GAP}px`}
        rowGap="10"
        alignItems="start"
        justifyContent="center"
      >
        {aside && <Box gridColumnStart={{ base: "1", lg: "3" }}>{aside}</Box>}
        <VStack
          gridColumnStart={{ lg: "2" }}
          gridRowStart={{ lg: "1" }}
          alignItems="flex-start"
          divider={<StackDivider />}
          gap={6}
        >
          {children}
        </VStack>
      </Grid>
    </Box>
  );
};
