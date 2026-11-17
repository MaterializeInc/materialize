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

import { InfoIcon, WarningIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { OntologyIssue } from "./model";
import { Selection } from "./OntologyCanvas";

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
            colors.background[
              issue.severity === "suggestion"
                ? "info"
                : issue.severity === "error"
                  ? "error"
                  : "warn"
            ]
          }
          border="1px solid"
          borderColor={
            colors.border[
              issue.severity === "suggestion"
                ? "info"
                : issue.severity === "error"
                  ? "error"
                  : "warn"
            ]
          }
        >
          <Box
            pt="2px"
            color={
              issue.severity === "suggestion"
                ? colors.accent.blue
                : issue.severity === "error"
                  ? colors.accent.red
                  : colors.accent.darkYellow
            }
          >
            {issue.severity === "suggestion" ? (
              <InfoIcon width="14px" height="14px" />
            ) : (
              <WarningIcon width="14px" height="14px" />
            )}
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
  onClick,
  isActive = false,
}: {
  value: number;
  label: string;
  color?: string;
  onClick?: () => void;
  isActive?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const clickable = onClick
    ? {
        as: "button" as const,
        onClick,
        "aria-pressed": isActive,
        textAlign: "left" as const,
        px: 1,
        py: 1,
        borderRadius: "md",
        background: isActive ? colors.background.accent : undefined,
        _hover: { background: colors.background.secondary },
      }
    : {};
  return (
    <VStack align="start" spacing={0} {...clickable}>
      <Text textStyle="heading-md" color={color ?? colors.foreground.primary}>
        {value}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {label}
      </Text>
    </VStack>
  );
};

/** Issues as buttons selecting the edge or entity each is about. */
export const SelectableIssues = ({
  issues,
  select,
}: {
  issues: OntologyIssue[];
  select: (selection: Selection) => void;
}) => (
  <>
    {issues.map((issue, i) => (
      <Box
        key={i}
        as="button"
        textAlign="left"
        onClick={() => {
          if (issue.nodeId) select({ type: "node", id: issue.nodeId });
          else if (issue.edgeId) select({ type: "edge", id: issue.edgeId });
        }}
      >
        <IssueList issues={[issue]} />
      </Box>
    ))}
  </>
);
