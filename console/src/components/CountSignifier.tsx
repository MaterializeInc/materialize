// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, TextProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

const CountSignifier = ({ children, ...rest }: TextProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text
      as="span"
      backgroundColor={colors.accent.brightPurple}
      borderRadius="36px"
      color={colors.white}
      textStyle="text-small"
      fontWeight="500"
      px="6px"
      h="4"
      w="5"
      ml="3"
      verticalAlign="top"
      {...rest}
    >
      {children}
    </Text>
  );
};

export default CountSignifier;
