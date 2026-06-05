// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Card,
  Divider,
  HStack,
  Tag,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { calculateBucketSizeFromLookback } from "~/api/materialize/freshness/lagHistory";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { MaterializeTheme } from "~/theme";
import { formatDate } from "~/utils/dateFormat";

import { LOOKBACK_OPTIONS } from "./constants";
import { CriticalPathGraph } from "./CriticalPathGraph";
import { computeFreshnessBreakdown } from "./freshnessBreakdown";
import { FreshnessBreakdownStrip } from "./FreshnessBreakdownStrip";
import { peakLagTimestampFromHistory } from "./freshnessHistory";
import { ObjectFreshnessChart } from "./ObjectFreshnessChart";
import {
  MaintainedObjectListItem,
  useCriticalPath,
  useObjectFreshnessHistory,
} from "./queries";

export interface ObjectFreshnessProps {
  item: MaintainedObjectListItem;
  timePeriodMinutes: number;
  setTimePeriodMinutes: React.Dispatch<React.SetStateAction<number>>;
}

export const ObjectFreshness = ({
  item,
  timePeriodMinutes,
  setTimePeriodMinutes,
}: ObjectFreshnessProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [lockedTimestamp, setLockedTimestamp] = React.useState<Date | null>(
    null,
  );

  // A lock from a previous lookback can fall outside the new window,
  // leaving the marker off-chart and the chain query empty. Reset on change.
  React.useEffect(() => {
    setLockedTimestamp(null);
  }, [timePeriodMinutes]);

  const bucketSizeMs = calculateBucketSizeFromLookback(
    timePeriodMinutes * 60 * 1000,
  );

  // Default anchor: the bucket where this object's lag peaked in the
  // window. The history call is already cached by `ObjectFreshnessChart`,
  // so React Query dedupes it.
  const { data: freshnessHistory } = useObjectFreshnessHistory({
    objectId: item.id,
    lookbackMs: timePeriodMinutes * 60 * 1000,
  });
  const autoAnchorTimestamp = peakLagTimestampFromHistory(
    freshnessHistory?.historicalData ?? [],
    item.id,
  );

  const isLocked = lockedTimestamp !== null;
  const anchorTimestamp = lockedTimestamp ?? autoAnchorTimestamp;

  const { data: criticalPath } = useCriticalPath({
    objectId: item.id,
    timestamp: anchorTimestamp,
    bucketSizeMs,
    lookbackMinutes: timePeriodMinutes,
  });
  const breakdown = computeFreshnessBreakdown(criticalPath?.directInputs ?? []);

  // Sources ingest from external systems and have no Materialize-internal
  // upstream chain to walk — hide the critical path section for them.
  const hasUpstreamChain = item.objectType !== "source";

  return (
    <VStack align="start" spacing={6} width="100%">
      <Card
        p={5}
        width="100%"
        borderRadius="md"
        border="1px"
        borderColor={colors.border.primary}
      >
        <VStack align="start" spacing={4} width="100%">
          <HStack width="100%" justify="space-between" align="start">
            <VStack align="start" spacing={1}>
              <Text textStyle="heading-sm">Freshness</Text>
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                The graph below shows the highest latencies over time. Pick a
                point on the freshness graph to visualize the critical path of
                lag on upstream dependencies on this object.
              </Text>
            </VStack>
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
              options={LOOKBACK_OPTIONS}
            />
          </HStack>

          <ObjectFreshnessChart
            objectId={item.id}
            timePeriodMinutes={timePeriodMinutes}
            onTimestampLock={setLockedTimestamp}
            selectedTimestamp={anchorTimestamp}
            isLocked={isLocked}
          />

          {hasUpstreamChain && (
            <>
              <Divider />

              <HStack width="100%" justify="space-between" align="center">
                <Text textStyle="heading-sm">Critical path</Text>
                {anchorTimestamp && (
                  <Tag
                    size="sm"
                    borderRadius="md"
                    bg={
                      isLocked
                        ? colors.background.accent
                        : colors.background.info
                    }
                    color={
                      isLocked ? colors.accent.brightPurple : colors.accent.blue
                    }
                  >
                    {isLocked ? "Locked at " : "Peak at "}
                    {formatDate(anchorTimestamp, "h:mm:ss a")}
                  </Tag>
                )}
              </HStack>

              {breakdown && <FreshnessBreakdownStrip breakdown={breakdown} />}

              <CriticalPathGraph
                probe={item}
                timestamp={anchorTimestamp}
                bucketSizeMs={bucketSizeMs}
                lookbackMinutes={timePeriodMinutes}
              />
            </>
          )}
        </VStack>
      </Card>
    </VStack>
  );
};
