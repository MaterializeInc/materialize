// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Card, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import TimePeriodSelect from "~/components/TimePeriodSelect";
import { MaterializeTheme } from "~/theme";

import { LOOKBACK_OPTIONS } from "./constants";
import { ObjectFreshnessChart } from "./ObjectFreshnessChart";
import { MaintainedObjectListItem } from "./queries";

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
                The graph below shows the highest latencies over time.
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
        </VStack>
      </Card>
    </VStack>
  );
};
