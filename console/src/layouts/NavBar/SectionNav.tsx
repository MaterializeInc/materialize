// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StackProps, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const SectionNav = (props: StackProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      bg={colors.background.secondary}
      borderRightWidth={1}
      borderColor={colors.border.primary}
      alignItems="stretch"
      alignSelf="stretch"
      height="100%"
      flexShrink="0"
      px="2"
      py="6"
      spacing="2"
      width="272px"
      {...props}
    />
  );
};
