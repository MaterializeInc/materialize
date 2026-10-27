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
  CloseButton,
  Flex,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  Grid,
  GridProps,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { NavLink } from "react-router-dom";

import { FIXED_TOP_BAR_Z_INDEX } from "~/layouts/zIndex";
import { MaterializeTheme } from "~/theme";

/**
 * Reusable form components
 *
 * Example usage:
 *
 * ```
 * <FormTopBar title="New Object" backButtonHref="..">
 *   <Button variant="primary"  type="submit">Create</Button>
 * </FormTopBar>
 * <FormContainer
 *   title="Create a thing"
 *   aside={
 *     <FormInfoBox>
 *       Some info
 *     </FormInfoBox>
 *   }
 * >
 *   <FormSection title="General">
 *     <FormControl>
 *       <InlineLabeledInput label="Name" error={formState.errors.name}>
 *       <Input />
 *       </InlineLabeledInput>
 *     </FormControl>
 *   </FormSection>
 * </FormContainer>
 * ```
 */

const FORM_COLUMN_GAP = 60;

export interface FormTopBarProps {
  title: string;
  backButtonHref: string;
}

export const FormTopBar = ({
  title,
  backButtonHref,
  children,
}: React.PropsWithChildren<FormTopBarProps>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Flex
      position="fixed"
      zIndex={FIXED_TOP_BAR_Z_INDEX}
      top="0"
      width="100%"
      backgroundColor={colors.background.primary}
      alignItems="center"
      justifyContent="space-between"
      px="4"
      py="3"
      boxSizing="border-box"
      borderBottom="1px solid"
      borderBottomColor={colors.border.primary}
    >
      <Flex alignItems="center">
        <Box pr="4" mr="4" borderRight={`1px solid ${colors.border.secondary}`}>
          <CloseButton
            as={NavLink}
            to={backButtonHref}
            height="24px"
            width="24px"
          />
        </Box>
        <Text fontWeight="500" fontSize="14px" lineHeight="16px">
          {title}
        </Text>
      </Flex>
      {children}
    </Flex>
  );
};

export interface FormSectionProps extends BoxProps {
  title: string;
  variant?: "narrow" | "full-page";
}

export const FormSection = ({
  title,
  children,
  ...props
}: React.PropsWithChildren<FormSectionProps>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const variant = props.variant ?? "full-page";

  return (
    <Box mb={variant === "full-page" ? "10" : "2"} {...props}>
      <Text
        as="legend"
        textStyle="heading-xs"
        color={colors.foreground.tertiary}
        mb={variant === "full-page" ? "6" : "4"}
      >
        {title}
      </Text>
      {children}
    </Box>
  );
};

export const FormInfoBox = ({
  children,
  ...props
}: React.PropsWithChildren<BoxProps>) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box
      flex="1"
      borderLeft={{
        base: "none",
        md: `1px solid ${colors.border.primary}`,
      }}
      px={{ base: "0", md: "6" }}
      py={{ base: "0", md: "4" }}
      as="aside"
      mr={{ base: "0", md: "20" }}
      gridColumnStart={{ base: "2", md: "auto" }}
      gridRowStart={{ base: "2", md: "auto" }}
      {...props}
    >
      {children}
    </Box>
  );
};

export interface FormContainerProps {
  title: string;
  aside?: React.ReactElement;
}

export const FormContainer = ({
  title,
  children,
  aside,
}: React.PropsWithChildren<FormContainerProps>) => {
  return (
    <Box mt={24}>
      <Grid
        templateColumns={{ md: "1fr 420px 1fr", base: "0 1fr 0" }}
        templateRows={{ md: "auto 1fr", base: "auto auto 1fr " }}
        columnGap={`${FORM_COLUMN_GAP}px`}
        rowGap="10"
        alignItems="start"
        justifyContent="center"
      >
        <Box gridColumnStart="2">
          <Text as="h1" fontSize="20px" fontWeight="600" lineHeight="24px">
            {title}
          </Text>
        </Box>
        <Box gridColumnStart="2">{children}</Box>
        {aside}
      </Grid>
    </Box>
  );
};

export const GutterContainer = ({ children }: React.PropsWithChildren) => {
  return (
    <Flex
      justifyContent="center"
      position="absolute"
      right={`-${FORM_COLUMN_GAP}px`}
      width={`${FORM_COLUMN_GAP}px`}
      height={`${FORM_COLUMN_GAP}px`}
      p="0"
    >
      {children}
    </Flex>
  );
};

export interface InlineLabeledInputProps {
  error?: string;
  label: string;
  message?: React.ReactNode;
  required?: boolean;
}

export const InlineInputContainer = (props: GridProps) => {
  return (
    <Grid
      templateColumns="min-content minmax(auto, 320px)"
      columnGap="6"
      justifyContent="space-between"
      alignItems="start"
      width="100%"
      position="relative"
      {...props}
    />
  );
};

export const InlineLabeledInput = ({
  children,
  error,
  label,
  message,
  required,
}: React.PropsWithChildren<InlineLabeledInputProps>) => {
  return (
    <InlineInputContainer>
      <FormLabel variant="inline" mt="2" lineHeight="16px">
        {label}
        {required ? "*" : ""}
      </FormLabel>
      {children}
      <Box gridColumn="2">
        {message && <FormHelperText>{message}</FormHelperText>}
        <FormErrorMessage>{error}</FormErrorMessage>
      </Box>
    </InlineInputContainer>
  );
};
