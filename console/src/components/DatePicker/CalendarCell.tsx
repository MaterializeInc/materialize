// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ButtonProps } from "@chakra-ui/button";
import { Box, chakra, Stack, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { TextStyles } from "~/theme/typography";

export type Variant =
  | "default"
  | "anchorSingle"
  | "anchorStart"
  | "anchorEnd"
  | "selectedSingle"
  | "selectedStart"
  | "selectedEnd"
  | "withinRange";

type VariantStyles = {
  [keyof in Variant]: {
    buttonBackgroundColor?: string;
    backgroundColor?: string;
    textStyle?: keyof TextStyles;
    textColor?: string;
    backgroundBorderRadius?: string;
    buttonBoxShadow?: string;
  };
};

// We can't use a border-radius of lg for the outer border because it won't fully wrap.
const OUTER_BORDER = "10px";

const BORDER_WIDTH = "2px";

const getVariantStyles: (theme: {
  colors: MaterializeTheme["colors"];
  shadows: MaterializeTheme["shadows"];
}) => VariantStyles = ({ colors, shadows }) => {
  const selectedPointStyles = {
    buttonBackgroundColor: colors.accent.brightPurple,
    backgroundColor: colors.background.info,
    textStyle: "text-ui-reg",
    textColor: colors.foreground.inverse,
  };

  const selectedStartStyles = {
    ...selectedPointStyles,
    backgroundBorderRadius: `${OUTER_BORDER} 0 0 ${OUTER_BORDER}`,
  };

  const selectedEndStyles = {
    ...selectedPointStyles,
    backgroundBorderRadius: `0 ${OUTER_BORDER} ${OUTER_BORDER} 0`,
  };

  const variantStyles = {
    default: {
      backgroundColor: colors.background.primary,
      textStyle: "text-ui-reg",
      textColor: colors.foreground.primary,
    },
    selectedSingle: selectedPointStyles,
    selectedStart: selectedStartStyles,
    selectedEnd: selectedEndStyles,
    anchorSingle: selectedPointStyles,
    anchorStart: selectedStartStyles,
    anchorEnd: selectedEndStyles,
    withinRange: {
      textStyle: "text-ui-reg",
      textColor: colors.foreground.primary,
      backgroundColor: colors.background.info,
      buttonBackgroundColor: colors.background.info,
      backgroundBorderRadius: "0",
    },
  };

  return variantStyles;
};

type CalendarCellProps = {
  buttonProps?: ButtonProps;
  variant: Variant;
  children?: React.ReactNode;
};

const CalendarCell = ({
  buttonProps,
  variant,
  children,
}: CalendarCellProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const variantStyles = getVariantStyles({ colors, shadows })[variant];

  const isDisabled = !!buttonProps?.disabled;

  return (
    <chakra.button
      _disabled={{
        cursor: "not-allowed",
        bg: "transparent",
        color: colors.foreground.tertiary,
      }}
      position="relative"
      outline="none"
      w="8"
      h="8"
      {...buttonProps}
    >
      <Box
        // Used for creating the ring around the button on hover
        position="absolute"
        w="100%"
        h="100%"
        top="0"
        left="0"
        borderWidth={BORDER_WIDTH}
        borderRadius={OUTER_BORDER}
        borderColor="transparent"
        _hover={{
          borderColor: !buttonProps?.disabled
            ? colors.border.info
            : "transparent",
          transition: "border-color 0.2s ease-in-out",
        }}
      />
      <Stack
        // Used for creating the backgrounds around the border
        backgroundColor={variantStyles?.backgroundColor}
        borderWidth={BORDER_WIDTH}
        borderRadius={variantStyles.backgroundBorderRadius ?? OUTER_BORDER}
        borderColor="transparent"
        w="100%"
        h="100%"
      >
        <Stack
          // The styling of the button
          borderColor="transparent"
          borderWidth={BORDER_WIDTH}
          borderRadius="lg"
          backgroundColor={variantStyles?.buttonBackgroundColor}
          shadow={variantStyles?.buttonBoxShadow}
          justifyContent="center"
          w="100%"
          h="100%"
        >
          <Text
            color={
              isDisabled ? colors.foreground.tertiary : variantStyles?.textColor
            }
            textStyle={variantStyles.textStyle as string}
          >
            {children}
          </Text>
        </Stack>
      </Stack>
    </chakra.button>
  );
};

export default CalendarCell;
