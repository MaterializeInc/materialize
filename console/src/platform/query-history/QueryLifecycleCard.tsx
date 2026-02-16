// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  AbsoluteCenter,
  Box,
  Divider,
  HStack,
  Spinner,
  Stack,
  Text,
  Tooltip,
  useInterval,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import { QueryHistoryStatementLifecycleRow } from "~/api/materialize/query-history/queryHistoryDetail";
import ErrorBox from "~/components/ErrorBox";
import { InfoIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { convertMillisecondsToDuration } from "~/util";
import { DATE_FORMAT, formatDate } from "~/utils/dateFormat";

import {
  LifecycleObject,
  STATEMENT_LIFECYCLE_POLL_RATE_MS,
  useFetchQueryHistoryStatementLifecycle,
} from "./queries";
import { formatDuration } from "./queryHistoryUtils";

const HeaderTextStack = ({
  title,
  dateSubtitle,
}: {
  title: string;
  dateSubtitle: Date;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack alignItems="flex-start" spacing="0" flexGrow="0">
      <Text
        textStyle="text-small"
        fontWeight="500"
        color={colors.foreground.secondary}
      >
        {title}
      </Text>
      <Text textStyle="text-ui-med">
        {formatDate(dateSubtitle, `${DATE_FORMAT} - HH:mm:ss.SSS z`)}
      </Text>
    </VStack>
  );
};

type StartedLifecycleObject = LifecycleObject & {
  "execution-began": QueryHistoryStatementLifecycleRow;
};

function isExecutionBeganDefined(
  lifecycle?: LifecycleObject,
): lifecycle is StartedLifecycleObject {
  return lifecycle?.["execution-began"] !== undefined;
}

export const Header = ({
  executionBeganAt,
  executionFinishedAt,
  elapsedTime,
}: {
  executionBeganAt: Date;
  executionFinishedAt?: Date;
  elapsedTime: string;
}) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  return (
    <VStack alignItems="flex-start" width="100%" spacing="2">
      <HStack spacing="6" width="100%">
        <HeaderTextStack title="Began at" dateSubtitle={executionBeganAt} />
        <Box flexGrow="1" position="relative">
          <Divider
            size="sm"
            borderStyle="dashed"
            color={colors.border.secondary}
            opacity="1"
            letterSpacing="16px"
          />
          <AbsoluteCenter
            background={colors.components.card.background}
            shadow={shadows.level2}
            px="3"
            py="1"
            borderRadius="xl"
          >
            <Text textStyle="text-ui-med" color={colors.foreground.primary}>
              {elapsedTime}
            </Text>
          </AbsoluteCenter>
        </Box>
        {executionFinishedAt ? (
          <HeaderTextStack
            title="Finished at"
            dateSubtitle={executionFinishedAt}
          />
        ) : (
          <HStack>
            <Spinner height="4" width="4" color={colors.accent.purple} />
            <Text textStyle="text-ui-med">In progress</Text>
          </HStack>
        )}
      </HStack>
    </VStack>
  );
};

function eventLifecycleRatioToFlexGrow(ratio: number) {
  // If the ratio's precision is > 3 and non-zero, we use the max of 0.01 to avoid flex-grow: 0
  return ratio > 0 ? Math.max(0.01, ratio).toFixed(2) : 0;
}

const EventTimeline = ({
  color,
  title,
  executionBeganAt,
  occurredAt,
  elapsedTimeMs,
  tooltipText,
  showBorderBottom = true,
}: {
  color: string;
  title: string;
  tooltipText?: string;
  occurredAt?: Date;
  executionBeganAt: Date;
  elapsedTimeMs: number;
  showBorderBottom?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const occurredAtMs = occurredAt?.getTime();
  const beganAtMs = executionBeganAt.getTime();

  // If the event hasn't occurred, we use the elapsed time since the event is currently happening
  const eventDurationMs = occurredAtMs
    ? occurredAtMs - beganAtMs
    : elapsedTimeMs;

  const eventDurationRatio = eventDurationMs / elapsedTimeMs;
  const eventDurationRatioComplement =
    (elapsedTimeMs - eventDurationMs) / elapsedTimeMs;

  const barFlexGrow = eventLifecycleRatioToFlexGrow(eventDurationRatio);

  const barComplementFlexGrow = eventLifecycleRatioToFlexGrow(
    eventDurationRatioComplement,
  );

  if (eventDurationMs === 0) {
    return null;
  }

  return (
    <HStack
      width="100%"
      spacing="2"
      borderBottomColor={colors.border.primary}
      borderBottomWidth={showBorderBottom ? "1px" : undefined}
    >
      <HStack spacing="2" width="248px" height="100%" py="2">
        <Box
          height="3"
          width="3"
          backgroundColor={color}
          borderRadius="base"
          flexShrink="0"
        />
        <Text textStyle="text-ui-med">{title}</Text>
        {tooltipText && (
          <Tooltip label={tooltipText}>
            <InfoIcon />
          </Tooltip>
        )}
      </HStack>

      <HStack spacing="0" flexGrow="1" marginLeft="2" height="100%">
        <Box
          height="2"
          flexGrow={barFlexGrow}
          borderRadius="sm"
          backgroundColor={color}
        />
        {occurredAt && (
          <Text
            flexShrink="0"
            flexGrow="0"
            marginLeft="2"
            textStyle="text-small"
          >
            {formatDuration(convertMillisecondsToDuration(eventDurationMs))}
          </Text>
        )}
        <Box flexGrow={barComplementFlexGrow} />
      </HStack>
    </HStack>
  );
};

const LifecycleEvents = ({
  lifecycle,
}: {
  lifecycle: StartedLifecycleObject;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [currentTimeMs, setCurrentTimeMs] = useState(new Date().getTime());

  const executionBeganAt = lifecycle["execution-began"].occurredAt;
  const executionFinishedAt = lifecycle["execution-finished"]?.occurredAt;
  const computeDependenciesFinishedAt =
    lifecycle["compute-dependencies-finished"]?.occurredAt;
  const storageDependenciesFinishedAt =
    lifecycle["storage-dependencies-finished"]?.occurredAt;
  const optimizationFinishedAt = lifecycle["optimization-finished"]?.occurredAt;

  const executionBeganAtMs = executionBeganAt.getTime();
  const executionFinishedAtMs = executionFinishedAt?.getTime() ?? 0;

  const computeDependenciesFinishedAtMs =
    computeDependenciesFinishedAt?.getTime() ?? 0;
  const storageDependenciesFinishedAtMs =
    storageDependenciesFinishedAt?.getTime() ?? 0;
  const optimizationFinishedAtMs = optimizationFinishedAt?.getTime() ?? 0;

  const isExecutionFinished = lifecycle?.["execution-finished"] !== undefined;

  const lastEventOccurredAtMs = Math.max(
    executionBeganAtMs,
    executionFinishedAtMs,
    computeDependenciesFinishedAtMs,
    storageDependenciesFinishedAtMs,
    optimizationFinishedAtMs,
  );

  useInterval(
    () => {
      setCurrentTimeMs(new Date().getTime());
    },
    // Turn off current time clock when execution is finished
    executionFinishedAt ? null : STATEMENT_LIFECYCLE_POLL_RATE_MS,
  );

  const currentTimeMarker = isExecutionFinished
    ? executionFinishedAtMs
    : Math.max(currentTimeMs, lastEventOccurredAtMs);

  const elapsedTimeMs = currentTimeMarker - executionBeganAtMs;

  const elapsedTimeRoundedToNearestSecond =
    Math.floor(elapsedTimeMs / 1000) * 1000;

  const elapsedTime = formatDuration(
    convertMillisecondsToDuration(
      // When using a timer to calculate the elapsed time, we round to the nearest second
      isExecutionFinished ? elapsedTimeMs : elapsedTimeRoundedToNearestSecond,
    ),
  );

  return (
    <VStack alignItems="flex-start" width="100%" height="100%" spacing="6">
      <Header
        executionBeganAt={executionBeganAt}
        executionFinishedAt={executionFinishedAt}
        elapsedTime={elapsedTime}
      />

      <VStack spacing="0" width="100%">
        {optimizationFinishedAt && (
          <EventTimeline
            title="Query optimization"
            color={colors.accent.blue}
            elapsedTimeMs={elapsedTimeMs}
            occurredAt={optimizationFinishedAt}
            executionBeganAt={executionBeganAt}
          />
        )}
        {storageDependenciesFinishedAt && (
          <EventTimeline
            title="Waiting for sources"
            color={colors.accent.purple}
            elapsedTimeMs={elapsedTimeMs}
            occurredAt={storageDependenciesFinishedAt}
            executionBeganAt={executionBeganAt}
            tooltipText="Time for source dependencies to become up to date"
          />
        )}

        {computeDependenciesFinishedAt && (
          <EventTimeline
            title="Waiting for compute"
            color={colors.accent.brightPurple}
            elapsedTimeMs={elapsedTimeMs}
            occurredAt={computeDependenciesFinishedAt}
            executionBeganAt={executionBeganAt}
            tooltipText="Time for index and materialized view dependencies to become up to date"
          />
        )}

        <EventTimeline
          title="Query execution"
          color={colors.border.secondary}
          elapsedTimeMs={elapsedTimeMs}
          occurredAt={executionFinishedAt}
          executionBeganAt={executionBeganAt}
          showBorderBottom={
            !!(
              storageDependenciesFinishedAt ||
              computeDependenciesFinishedAt ||
              optimizationFinishedAt
            )
          }
        />
      </VStack>
    </VStack>
  );
};

const QueryLifecycleCard = ({ executionId = "" }: { executionId?: string }) => {
  const {
    colors: { components },
    shadows,
  } = useTheme<MaterializeTheme>();

  const {
    data,
    isError,
    isLoading: isStatementLifecycleLoading,
  } = useFetchQueryHistoryStatementLifecycle({
    executionId,
  });

  const isLoading =
    isStatementLifecycleLoading || !isExecutionBeganDefined(data);

  return (
    <Box
      shadow={shadows.level1}
      background={components.card.background}
      borderRadius="lg"
      py="6"
      px="10"
      width="100%"
    >
      {isError ? (
        <ErrorBox />
      ) : isLoading ? (
        <Stack
          width="100%"
          height="104px"
          alignItems="center"
          justifyContent="center"
        >
          <Spinner />
        </Stack>
      ) : (
        <VStack alignItems="flex-start" width="100%" height="100%" spacing="6">
          <LifecycleEvents lifecycle={data} />
        </VStack>
      )}
    </Box>
  );
};

export default QueryLifecycleCard;
