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
  Card,
  Flex,
  HStack,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { TooltipColorSwatch } from "~/components/graphComponents";
import TextLink from "~/components/TextLink";
import { useFlags } from "~/hooks/useFlags";
import { LargestReplicaBoundary } from "~/platform/clusters/LargestMaintainedQueries";
import { useLargestMaintainedQueries } from "~/platform/clusters/queries";
import {
  maintainedObjectPath,
  useBuildWorkflowGraphPath,
} from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { formatMemoryUsage } from "~/utils/format";

import { segmentWidths } from "./memoryByObjectModel";
import { PERCENT_TICKS } from "./ResourceUsage/resourceUsageStyles";

export interface MemoryByObjectProps {
  clusterId: string;
  clusterName: string;
}

/** The largest objects on the cluster's largest replica, as shares of its heap limit. */
export const MemoryByObject = ({
  clusterId,
  clusterName,
}: MemoryByObjectProps) => (
  <LargestReplicaBoundary clusterId={clusterId}>
    {(replica) => (
      <MemoryByObjectCard
        clusterId={clusterId}
        clusterName={clusterName}
        replicaName={replica.name}
        replicaHeapLimit={replica.heapLimit}
      />
    )}
  </LargestReplicaBoundary>
);

const MemoryByObjectCard = ({
  clusterId,
  clusterName,
  replicaName,
  replicaHeapLimit,
}: MemoryByObjectProps & { replicaName: string; replicaHeapLimit: number }) => {
  const { colors } = useTheme<MaterializeTheme>();
  // The maintained objects page only has routes while its flag is on.
  const hasMaintainedObjectsPage = Boolean(
    useFlags()["maintained-objects-ui-50"],
  );
  const regionSlug = useRegionSlug();
  const workflowGraphPath = useBuildWorkflowGraphPath();
  const { data: objects } = useLargestMaintainedQueries({
    clusterId,
    clusterName,
    replicaName,
    replicaHeapLimit,
  });

  if (!objects || objects.length === 0) return null;

  const widths = segmentWidths(
    objects.map((object) => object.memoryPercentage),
  );
  const segments = objects.map((object, index) => {
    const { id, name, databaseName, schemaName } = object;
    return {
      key: id ?? object.dataflowId ?? String(index),
      label: name ?? object.dataflowName ?? "-",
      usage: formatMemoryUsage(object),
      color: colors.lineGraph[index % colors.lineGraph.length],
      widthPercent: widths[index],
      path:
        object.isOrphanedDataflow || !id || !name || !schemaName
          ? undefined
          : hasMaintainedObjectsPage
            ? maintainedObjectPath(regionSlug, id)
            : workflowGraphPath({
                databaseObject: { id, name, databaseName, schemaName },
                type: object.type,
              }),
    };
  });

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      borderWidth="1px"
      borderColor={colors.border.primary}
    >
      <VStack spacing={4} alignItems="stretch">
        <HStack spacing={2} alignItems="baseline" flexWrap="wrap">
          <Text as="h3" textStyle="heading-sm">
            Memory consumption by object ({objects.length})
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            % of heap limit on {replicaName}
          </Text>
        </HStack>
        <Box>
          <Flex
            role="img"
            aria-label="Memory by object"
            height="6"
            borderRadius="sm"
            overflow="hidden"
            background={colors.background.secondary}
          >
            {segments.map(
              (segment) =>
                segment.widthPercent > 0 && (
                  <Tooltip
                    key={segment.key}
                    label={`${segment.label} · ${segment.usage}`}
                  >
                    <Box
                      width={`${segment.widthPercent}%`}
                      background={segment.color}
                      borderRightWidth="1px"
                      borderColor={colors.background.primary}
                    />
                  </Tooltip>
                ),
            )}
          </Flex>
          <Box position="relative" height="4" mt={1}>
            {PERCENT_TICKS.map((tick) => (
              <Text
                key={tick}
                position="absolute"
                left={`${tick}%`}
                transform={
                  tick === 0
                    ? undefined
                    : tick === 100
                      ? "translateX(-100%)"
                      : "translateX(-50%)"
                }
                textStyle="text-small"
                color={colors.foreground.secondary}
              >
                {tick}%
              </Text>
            ))}
          </Box>
        </Box>
        <Flex
          as="ul"
          aria-label="Objects"
          flexWrap="wrap"
          columnGap={6}
          rowGap={2}
          listStyleType="none"
        >
          {segments.map((segment) => (
            <HStack as="li" key={segment.key} spacing={1.5}>
              <TooltipColorSwatch color={segment.color} />
              {segment.path ? (
                <TextLink
                  as={RouterLink}
                  to={segment.path}
                  textStyle="text-small"
                >
                  {segment.label}
                </TextLink>
              ) : (
                <Text textStyle="text-small">{segment.label}</Text>
              )}
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                whiteSpace="nowrap"
              >
                {segment.usage}
              </Text>
            </HStack>
          ))}
        </Flex>
      </VStack>
    </Card>
  );
};
