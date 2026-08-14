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
  HStack,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react"; 
import { MaterializeTheme } from "~/theme";

export interface PercentBarProps {
  value: number | null | undefined;
}

const getPercentColor = (
  percent: number,
  colors: MaterializeTheme["colors"],
) => {
  if (percent > 90) return colors.accent.red;
  if (percent > 70) return colors.accent.orange;
  return colors.accent.green;
};

const PercentBar = ({ value }: PercentBarProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (value === null || value === undefined) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  const rounded = Math.round(value);
  const barColor = getPercentColor(rounded, colors);

  return (
    <HStack spacing="2" minWidth="80px">
      <Text textStyle="text-ui-med" whiteSpace="nowrap" minWidth="32px">
        {rounded}%
      </Text>
      <Box
        width="48px"
        height="8px"
        borderRadius="full"
        bg={colors.background.secondary}
        overflow="hidden"
        flexShrink={0}
      >
        <Box
          height="100%"
          width={`${Math.min(rounded, 100)}%`}
          borderRadius="full"
          bg={barColor}
          transition="width 0.3s ease"
        />
      </Box>
    </HStack>
  );
};
export default PercentBar;