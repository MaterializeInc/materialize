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
  Heading,
  HStack,
  StackProps,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { forwardRef } from "react";
import { LinkProps } from "react-router-dom";

import { MaterializeTheme } from "~/theme";
import { kebabToTitleCase } from "~/util";

export type AlertVariant = "error" | "info" | "warning" | "success";

export interface AlertProps extends BoxProps {
  variant: AlertVariant;
  children?: React.ReactNode;
  showLabel?: boolean;
  label?: React.ReactNode;
  message?: React.ReactNode;
  showButton?: boolean;
  buttonText?: string;
  buttonProps?: ButtonProps & Partial<LinkProps>;
}

function getColorScheme(
  colors: MaterializeTheme["colors"],
): Record<AlertVariant, { border: string; background: string }> {
  return {
    error: {
      border: colors.border.error,
      background: colors.background.error,
    },
    info: {
      border: colors.border.info,
      background: colors.background.info,
    },
    warning: {
      border: colors.border.warn,
      background: colors.background.warn,
    },
    success: {
      border: colors.accent.darkGreen,
      background: colors.accent.darkGreen,
    },
  };
}

export const Alert = forwardRef(
  (
    {
      variant,
      children,
      showLabel = true,
      label = "",
      message = "",
      showButton = false,
      buttonText = "View details",
      buttonProps = {},
      ...props
    }: AlertProps,
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();

    const colorScheme = getColorScheme(colors);

    return (
      <Box
        borderRadius="lg"
        borderColor={colorScheme[variant].border}
        borderWidth="1px"
        overflow="hidden"
        ref={ref}
        minWidth="400px"
        {...props}
      >
        <Box py="4" px="6" background={colorScheme[variant].background}>
          <HStack spacing={6} justifyContent="space-between">
            <VStack spacing="1" alignItems="start">
              {showLabel ? (
                <Heading
                  as="h6"
                  fontSize="sm"
                  lineHeight="16px"
                  fontWeight="500"
                  color={colors.foreground.primary}
                >
                  {label ? label : kebabToTitleCase(variant)}
                </Heading>
              ) : null}
              <Text
                as="div"
                fontSize="sm"
                lineHeight="20px"
                color={colors.foreground.primary}
              >
                {message}
              </Text>
            </VStack>
            {showButton ? (
              <Box
                height="100%"
                borderLeft="1px solid"
                borderColor={colorScheme[variant].border}
                pl={4}
              >
                <Button
                  variant="text-only"
                  textStyle="text-ui-med"
                  {...buttonProps}
                >
                  {buttonText}
                </Button>
              </Box>
            ) : null}
          </HStack>
        </Box>
        {children}
      </Box>
    );
  },
);

/**
 * A full screen banner that is displayed at the top of the page.
 * A banner that spans the full width of the page and is displayed at the top.
 * Can be used with AlertBannerTitle and AlertBannerDescription components to structure the content.
 *
 * @example
 * ```tsx
 * <AlertBanner variant="error">
 *   <AlertBannerContent variant="error" title="Important Notice">Your trial expires in 3 days</AlertBannerTitle>
 * </AlertBanner>
 * ```
 */
export const AlertBanner = forwardRef(
  (
    {
      variant,
      ...props
    }: StackProps & {
      variant: AlertVariant;
    },
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();

    const colorScheme = getColorScheme(colors);

    return (
      <HStack
        backgroundColor={colorScheme[variant].background}
        borderTopWidth="1px"
        borderTopColor={colorScheme[variant].border}
        borderBottomWidth="1px"
        borderBottomColor={colorScheme[variant].border}
        py="2"
        px="4"
        fontSize="sm"
        lineHeight="20px"
        color={colors.foreground.primary}
        justifyContent="center"
        width="100%"
        ref={ref}
        spacing="0"
        {...props}
      />
    );
  },
);

export const AlertBannerContent = ({
  children,
  title,
  variant,
}: {
  title: React.ReactNode;
  children: React.ReactNode;
  variant: AlertVariant;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const colorScheme = getColorScheme(colors);
  return (
    <>
      <Text
        textStyle="text-ui-med"
        borderRight="1px solid"
        borderColor={colorScheme[variant].border}
        paddingRight="6"
      >
        {title}
      </Text>
      <HStack paddingLeft="6" alignItems="start" flexGrow="1">
        {children}
      </HStack>
    </>
  );
};

export default Alert;
