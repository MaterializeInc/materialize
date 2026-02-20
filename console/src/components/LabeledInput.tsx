// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ChakraComponent,
  HStack,
  InputProps,
  StackProps,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { Input as InputTheme } from "~/theme/components";

interface LabeledInputProps {
  label?: React.ReactNode;
  labelContainerProps?: StackProps;
  containerProps?: StackProps;
  renderInput?: (inputStyles: InputProps) => React.ReactNode;
  inputProps?: ChakraComponent<"input", InputProps>;
}

/**
 * A component that renders an input with a label to the left.
 * We've decided not to use Chakra's InputGroup component given the focus styles applies only to the input
 * and not the entire container.
 */
const LabeledInput = ({
  label,
  labelContainerProps,
  containerProps,
  renderInput,
}: LabeledInputProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const inputStyles = {
    border: "none",
    borderTopLeftRadius: "0",
    borderBottomLeftRadius: "0",
    borderTopRightRadius: "lg",
    borderBottomRightRadius: "lg",
    _focus: {},
    _invalid: {},
  };

  return (
    <HStack
      _focusWithin={{
        ...InputTheme.variants?.default.field._focus,
      }}
      _invalid={{
        ...InputTheme.baseStyle?.field._invalid,
      }}
      borderRadius="lg"
      borderWidth="1px"
      borderColor={colors.border.secondary}
      spacing="0"
      {...containerProps}
    >
      <VStack
        borderRightWidth="1px"
        borderRightColor={colors.border.secondary}
        spacing="0"
        justifyContent="center"
        px="3"
        py="2"
        flexShrink="0"
        {...labelContainerProps}
      >
        {label}
      </VStack>

      {renderInput?.(inputStyles)}
    </HStack>
  );
};

export default LabeledInput;
