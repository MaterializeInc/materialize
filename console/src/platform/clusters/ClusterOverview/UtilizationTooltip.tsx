// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { TooltipInPortalProps } from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import React from "react";

import {
  GraphTooltip,
  GraphTooltipBucketRange,
} from "~/components/graphComponents";
import OverflowMenuIcon from "~/svg/OverflowMenuIcon";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDateInUtc, FRIENDLY_DATETIME_FORMAT } from "~/utils/dateFormat";

import { DataPoint, GraphDataKey, OfflineEvent } from "./types";

export interface UtilizationTooltipData {
  data: DataPoint[];
  nearestPointX: number;
}

export interface UtilizationTooltipProps {
  component: React.FC<TooltipInPortalProps>;
  dataKey: GraphDataKey;
  replicaColorMap: Map<string, { label: string; color: string }>;
  tooltipData: UtilizationTooltipData;
  left: number;
  top: number;
}

export const UtilizationTooltip = ({
  dataKey,
  replicaColorMap,
  tooltipData,
  ...props
}: UtilizationTooltipProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { data } = tooltipData;

  if (data.length === 0) {
    return null;
  }

  const { bucketStart, bucketEnd } = data[0];

  return (
    <GraphTooltip {...props}>
      {tooltipData.data.map((datapoint, i) => {
        const offlineEvents = [...datapoint.offlineEvents].sort(
          (a, b) => b.timestamp - a.timestamp,
        );

        const hasOomEvent = offlineEvents.some(
          (event) => event.offlineReason === "oom-killed",
        );
        return (
          <VStack key={i} align="start" spacing="1" width="100%">
            <Flex
              background={colors.background.secondary}
              borderColor={colors.border.primary}
              borderBottomWidth="1px"
              justifyContent="space-between"
              alignItems="center"
              width="100%"
              gap={4}
              paddingX="4"
              paddingY="1"
            >
              <div>
                <Text as="span" textStyle="text-ui-med">
                  {replicaColorMap.get(datapoint.id)?.label ?? datapoint.name}
                </Text>{" "}
                <Text
                  as="span"
                  color={colors.foreground.secondary}
                  textStyle="text-ui-med"
                >
                  {datapoint.size}
                </Text>
              </div>
              {hasOomEvent ? (
                <Text textStyle="text-ui-med">Out of Memory</Text>
              ) : (
                <Text textStyle="text-ui-med">
                  {datapoint[dataKey]
                    ? `${(datapoint[dataKey] as number | null)?.toFixed(1)}%`
                    : "-"}
                </Text>
              )}
            </Flex>
            {offlineEvents.length > 0 && (
              <Box
                borderColor={colors.border.primary}
                borderBottomWidth="1px"
                paddingX="4"
                paddingY="1"
                width="100%"
              >
                <Text
                  textStyle="text-small"
                  fontWeight="500"
                  color={colors.foreground.secondary}
                >
                  Events
                </Text>
                {offlineEvents.slice(0, 4).map((event) => (
                  <TooltipClusterEvent event={event} key={event.timestamp} />
                ))}
                {offlineEvents.length > 5 && (
                  <>
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      <OverflowMenuIcon />
                      {offlineEvents.length - 5} other{" "}
                      {pluralize(offlineEvents.length - 5, "change", "changes")}
                    </Text>
                    <TooltipClusterEvent
                      event={offlineEvents[offlineEvents.length - 1]}
                    />
                  </>
                )}
              </Box>
            )}
          </VStack>
        );
      })}

      <GraphTooltipBucketRange
        bucketStart={new Date(bucketStart)}
        bucketEnd={new Date(bucketEnd)}
      />
    </GraphTooltip>
  );
};

export const TooltipClusterEvent = ({ event }: { event: OfflineEvent }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <HStack spacing="2" justifyContent="space-between">
      <Text textStyle="text-ui-med">{event.offlineReason ?? event.status}</Text>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {formatDateInUtc(event.timestamp, FRIENDLY_DATETIME_FORMAT)}
      </Text>
    </HStack>
  );
};
