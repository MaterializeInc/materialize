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
import { ObjectFreshnessChart } from "./ObjectFreshnessChart";
import { MaintainedObjectListItem, useCriticalPath } from "./queries";

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
  const [hoverTimestamp, setHoverTimestamp] = React.useState<Date | null>(null);
  const [lockedTimestamp, setLockedTimestamp] = React.useState<Date | null>(
    null,
  );

  const isLocked = lockedTimestamp !== null;
  const selectedTimestamp = lockedTimestamp ?? hoverTimestamp;
  const bucketSizeMs = calculateBucketSizeFromLookback(
    timePeriodMinutes * 60 * 1000,
  );

  const { data: criticalPath } = useCriticalPath({
    objectId: item.id,
    timestamp: lockedTimestamp,
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
              <Text textStyle="heading-sm">Freshness latency</Text>
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
            onTimestampSelect={isLocked ? undefined : setHoverTimestamp}
            onTimestampLock={setLockedTimestamp}
            selectedTimestamp={selectedTimestamp}
            isLocked={isLocked}
          />

          {hasUpstreamChain && (
            <>
              <Divider />

              <HStack width="100%" justify="space-between" align="center">
                <Text textStyle="heading-sm">Critical path</Text>
                {isLocked ? (
                  <Tag
                    size="sm"
                    borderRadius="md"
                    bg={colors.background.accent}
                    color={colors.accent.brightPurple}
                  >
                    Locked at {formatDate(lockedTimestamp, "h:mm:ss a")}
                  </Tag>
                ) : (
                  <Tag
                    size="sm"
                    borderRadius="md"
                    bg={colors.background.info}
                    color={colors.accent.blue}
                  >
                    Live
                  </Tag>
                )}
              </HStack>

              {breakdown && <FreshnessBreakdownStrip breakdown={breakdown} />}

              <CriticalPathGraph
                probe={item}
                timestamp={lockedTimestamp}
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
