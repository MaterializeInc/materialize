// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  BoxProps,
  Input,
  InputGroup,
  InputLeftElement,
  InputProps,
  InputRightElement,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React, { forwardRef } from "react";

import { ChevronDownIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

export const DropdownToggleButton = forwardRef(
  (
    {
      inputProps,
      children,
      leftIcon,
      ...rest
    }: {
      children?: React.ReactNode;
      leftIcon?: React.ReactNode;
      inputProps?: InputProps;
    } & BoxProps,
    ref,
  ) => {
    const { colors } = useTheme<MaterializeTheme>();

    return (
      <InputGroup ref={ref} width="auto" outline="none" tabIndex={-1} {...rest}>
        <InputLeftElement w="10" pointerEvents="none">
          {leftIcon}
        </InputLeftElement>
        <Input
          as="button"
          backgroundColor={colors.background.primary}
          px="8"
          {...inputProps}
        >
          <Text noOfLines={1}>{children}</Text>
        </Input>
        <InputRightElement w="8" pointerEvents="none">
          <ChevronDownIcon color={colors.foreground.secondary} />
        </InputRightElement>
      </InputGroup>
    );
  },
);
