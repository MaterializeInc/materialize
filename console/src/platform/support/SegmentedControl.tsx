// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, useTheme, VisuallyHiddenInput } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

/**
 * A bordered row of radio buttons, the selected one filled. Put it in a
 * `<fieldset>` with a legend that names the choice.
 */
export const SegmentedControl = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex
      width="fit-content"
      borderWidth="1px"
      borderColor={colors.border.secondary}
      borderRadius="lg"
      overflow="hidden"
      sx={{ "& > label:not(:last-of-type)": { borderRightWidth: "1px" } }}
    >
      {children}
    </Flex>
  );
};

/**
 * One option of a `SegmentedControl`. `inputProps` go on the native radio
 * input: a react-hook-form registration, or `checked` and `onChange`.
 */
export const SegmentOption = ({
  inputProps,
  value,
  label,
  isSelected,
  size = "md",
}: {
  inputProps: React.ComponentPropsWithoutRef<"input"> & {
    ref?: React.Ref<HTMLInputElement>;
  };
  value: string;
  label: string;
  isSelected: boolean;
  size?: "sm" | "md";
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      as="label"
      display="inline-flex"
      alignItems="center"
      minHeight={size === "sm" ? "8" : "9"}
      px={size === "sm" ? "3" : "3.5"}
      cursor="pointer"
      fontWeight="500"
      textStyle="text-ui-med"
      borderColor={colors.border.secondary}
      bg={isSelected ? colors.accent.purple : colors.background.primary}
      color={isSelected ? colors.foreground.inverse : colors.foreground.primary}
      _focusWithin={{ boxShadow: "input.focus" }}
    >
      <VisuallyHiddenInput type="radio" value={value} {...inputProps} />
      {label}
    </Box>
  );
};
