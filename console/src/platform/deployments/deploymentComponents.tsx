// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Spinner, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { CopyButton } from "~/components/copyableComponents";
import StatusPill from "~/components/StatusPill";
import { MaterializeTheme } from "~/theme";

import { ClusterStatus, CutoverState, formatDuration } from "./deploymentModel";

const CLUSTER_STATUS_PILL: Record<
  ClusterStatus,
  { label: string; colorScheme: "green" | "lightBlue" | "yellow" | "red" }
> = {
  ready: { label: "Ready", colorScheme: "green" },
  hydrating: { label: "Hydrating", colorScheme: "lightBlue" },
  lagging: { label: "Lagging", colorScheme: "yellow" },
  failing: { label: "Failing", colorScheme: "red" },
};

export const ClusterStatusPill = ({ status }: { status: ClusterStatus }) => {
  const { label, colorScheme } = CLUSTER_STATUS_PILL[status];
  return (
    <StatusPill
      status={status}
      label={label}
      colorScheme={colorScheme}
      icon={
        status === "hydrating" ? (
          <Spinner width="12px" height="12px" speed="0.75s" />
        ) : undefined
      }
    />
  );
};

const CUTOVER_PILL: Record<
  CutoverState,
  { label: string; colorScheme: "green" | "yellow" | "red" }
> = {
  ready: { label: "Ready to switch", colorScheme: "green" },
  behind: { label: "Promote waits", colorScheme: "yellow" },
  stuck: { label: "Would hang", colorScheme: "red" },
  "no-replacement": { label: "Repoint fails", colorScheme: "red" },
};

export const CutoverPill = ({ state }: { state: CutoverState }) => {
  const { label, colorScheme } = CUTOVER_PILL[state];
  return <StatusPill status={state} label={label} colorScheme={colorScheme} />;
};

export const DeployStatePill = ({ promoted }: { promoted: boolean }) =>
  promoted ? (
    <StatusPill status="promoted" label="Promoted" colorScheme="gray" />
  ) : (
    <StatusPill status="in-progress" label="In progress" colorScheme="blue" />
  );

/** A shell command with a copy button. Console never runs the deploy. */
export const CommandSnippet = ({ command }: { command: string }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      spacing="1"
      pl="3"
      pr="1"
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="lg"
      background={colors.background.secondary}
    >
      <Text as="code" textStyle="monospace" fontSize="sm" whiteSpace="nowrap">
        {command}
      </Text>
      <CopyButton contents={command} size="sm" />
    </HStack>
  );
};

export const SectionHeading = ({
  title,
  description,
  children,
}: {
  title: string;
  description?: React.ReactNode;
  children?: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack justifyContent="space-between" alignItems="end" mb="3">
      <Box>
        <Text textStyle="heading-sm">{title}</Text>
        {description && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {description}
          </Text>
        )}
      </Box>
      {children}
    </HStack>
  );
};

export const Muted = ({ children }: { children: React.ReactNode }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text as="span" color={colors.foreground.secondary}>
      {children}
    </Text>
  );
};

export const LagText = ({ lagMs }: { lagMs: number | null | undefined }) =>
  lagMs === null || lagMs === undefined ? (
    <Muted>—</Muted>
  ) : (
    <>{formatDuration(lagMs)}</>
  );

/** Width of a bar relative to the longest value in a set. */
export const RelativeBar = ({
  fraction,
  color,
}: {
  fraction: number;
  color: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      width="120px"
      height="8px"
      borderRadius="full"
      bg={colors.background.tertiary}
      overflow="hidden"
      flexShrink={0}
    >
      <Box
        height="100%"
        width={`${Math.max(2, Math.min(1, fraction) * 100)}%`}
        borderRadius="full"
        bg={color}
      />
    </Box>
  );
};
