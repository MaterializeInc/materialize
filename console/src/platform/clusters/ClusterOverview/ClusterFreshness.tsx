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
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { createNamespace } from "~/api/materialize";
import {
  calculateBucketSizeFromLookback,
  NULL_LAG_TEXT,
} from "~/api/materialize/freshness/lagHistory";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { FreshnessGraph } from "~/components/FreshnessGraph/FreshnessGraph";
import { LoadingContainer } from "~/components/LoadingContainer";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { sumPostgresIntervalMs } from "~/util";
import { formatInterval, formatIntervalShort } from "~/utils/format";

import {
  CurrentClusterFreshnessData,
  LINE_MAX_COUNT,
  useClusterFreshness,
} from "../queries";

const TIME_PERIOD_OPTIONS = {
  "60": "Last hour",
  "180": "Last 3 hours",
  "360": "Last 6 hours",
  "1440": "Last 24 hours",
};

const HOUR_IN_MS = 60 * 60_000;

const ClusterFreshnessTable = ({
  data,
}: {
  data: CurrentClusterFreshnessData[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (data.length === 0) {
    return null;
  }

  return (
    <Table variant="linkable" borderRadius="xl" mt={4}>
      <Thead>
        <Tr>
          <Th>Name</Th>
          <Th>Freshness latency</Th>
        </Tr>
      </Thead>
      <Tbody>
        {data.map(({ objectId, objectName, lag, schemaName, databaseName }) => {
          let tableText = null;
          if (lag === null) {
            tableText = NULL_LAG_TEXT;
          } else {
            tableText =
              sumPostgresIntervalMs(lag) > HOUR_IN_MS
                ? formatIntervalShort(lag)
                : formatInterval(lag);
          }

          return (
            <Tr key={objectId}>
              <Td {...truncateMaxWidth} py="2">
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  noOfLines={1}
                  color={colors.foreground.secondary}
                >
                  {createNamespace(databaseName, schemaName)}
                </Text>
                <Text noOfLines={1}>{objectName}</Text>
              </Td>
              <Td>{tableText}</Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

const FreshnessGraphWrapper = ({
  lookbackMs,
  bucketSizeMs,
  clusterId,
}: {
  lookbackMs: number;
  bucketSizeMs: number;
  clusterId: string;
}) => {
  const {
    data: { historicalData, currentData, startTime, endTime, lines },
  } = useClusterFreshness({
    lookbackMs,
    clusterId,
  });

  return (
    <VStack alignItems="flex-start" width="100%" spacing="0" padding="4">
      <FreshnessGraph
        bucketSizeMs={bucketSizeMs}
        xAccessor={(d) => d.timestamp}
        lines={lines}
        data={historicalData}
        startTime={startTime}
        endTime={endTime}
        maxTooltipLines={LINE_MAX_COUNT}
      />
      <ClusterFreshnessTable data={currentData} />
    </VStack>
  );
};

const ClusterFreshness = ({ clusterId }: { clusterId: string }) => {
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-environment-overview-freshness-graph-time-period",
    timePeriodOptions: TIME_PERIOD_OPTIONS,
  });

  const lookbackMs = timePeriodMinutes * 60_000;

  const bucketSizeMs = calculateBucketSizeFromLookback(lookbackMs);

  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      borderRadius="lg"
      borderWidth="1px"
      spacing="0"
    >
      <VStack
        alignItems="flex-start"
        gap="1"
        width="100%"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        padding="4"
      >
        <HStack width="100%" justifyContent="space-between">
          <VStack alignItems="flex-start" gap="1">
            <HStack>
              <Text textStyle="heading-md" color={colors.foreground.primary}>
                Freshness latency
              </Text>
            </HStack>

            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Materialize continuously monitors how far dataflows lag behind the
              wall clock time. The graph below displays the dataflows with the
              highest latencies over time.
            </Text>
          </VStack>
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={(timePeriod) => {
              setTimePeriodMinutes(timePeriod);
            }}
            options={TIME_PERIOD_OPTIONS}
          />
        </HStack>
      </VStack>
      <AppErrorBoundary
        message="An error occurred fetching freshness latency data."
        containerProps={{
          padding: "4",
        }}
      >
        <React.Suspense
          fallback={
            <Box height="240px" width="100%">
              <LoadingContainer />
            </Box>
          }
        >
          <FreshnessGraphWrapper
            lookbackMs={lookbackMs}
            bucketSizeMs={bucketSizeMs}
            clusterId={clusterId}
          />
        </React.Suspense>
      </AppErrorBoundary>
    </VStack>
  );
};

export default ClusterFreshness;
