// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { WarningIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { OntologyIssue } from "./model";

export const IssueList = ({ issues }: { issues: OntologyIssue[] }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (issues.length === 0) return null;
  return (
    <VStack align="stretch" spacing={2}>
      {issues.map((issue, i) => (
        <HStack
          key={i}
          align="start"
          spacing={2}
          p={2.5}
          borderRadius="md"
          background={
            issue.severity === "error"
              ? colors.background.error
              : colors.background.warn
          }
          border="1px solid"
          borderColor={
            issue.severity === "error"
              ? colors.border.error
              : colors.border.warn
          }
        >
          <Box
            pt="2px"
            color={
              issue.severity === "error"
                ? colors.accent.red
                : colors.accent.darkYellow
            }
          >
            <WarningIcon width="14px" height="14px" />
          </Box>
          <Text textStyle="text-small">{issue.message}</Text>
        </HStack>
      ))}
    </VStack>
  );
};

export const Figure = ({
  value,
  label,
  color,
}: {
  value: number;
  label: string;
  color?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack align="start" spacing={0}>
      <Text textStyle="heading-md" color={color ?? colors.foreground.primary}>
        {value}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {label}
      </Text>
    </VStack>
  );
};
