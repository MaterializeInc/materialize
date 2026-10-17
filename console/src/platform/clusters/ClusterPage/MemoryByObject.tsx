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

import Alert from "~/components/Alert";
import { TooltipColorSwatch } from "~/components/graphComponents";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { generateRainbowPalette } from "~/components/ThresholdLineGraph/thresholdLineGraphHelpers";
import { LargestReplicaBoundary } from "~/platform/clusters/LargestMaintainedQueries";
import {
  UNIFIED_OBJECT_SIZES_MIN_VERSION,
  useLargestMaintainedQueries,
} from "~/platform/clusters/queries";
import { useEnvironmentGate } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { formatMemoryUsage } from "~/utils/format";

import {
  LargestObject,
  segmentWidths,
  TOP_OBJECT_COUNT,
} from "./memoryByObjectModel";
import { PERCENT_TICKS } from "./ResourceUsage/resourceUsageStyles";
import { useLiveLargestObjects } from "./useLiveLargestObjects";
import { useObjectLink } from "./useObjectLink";

export interface MemoryByObjectProps {
  clusterId: string;
  clusterName: string;
}

/** The largest objects on the cluster's largest replica, as shares of its heap limit. */
export const MemoryByObject = ({
  clusterId,
  clusterName,
}: MemoryByObjectProps) => {
  const hasLiveSizes =
    useEnvironmentGate(UNIFIED_OBJECT_SIZES_MIN_VERSION) === true;
  return (
    <LargestReplicaBoundary clusterId={clusterId}>
      {(replica) =>
        hasLiveSizes ? (
          <LiveMemoryByObject
            replicaId={replica.id}
            replicaName={replica.name}
            replicaHeapLimit={replica.heapLimit}
          />
        ) : (
          <PolledMemoryByObject
            clusterId={clusterId}
            clusterName={clusterName}
            replicaName={replica.name}
            replicaHeapLimit={replica.heapLimit}
          />
        )
      }
    </LargestReplicaBoundary>
  );
};

const PolledMemoryByObject = ({
  clusterId,
  clusterName,
  replicaName,
  replicaHeapLimit,
}: MemoryByObjectProps & { replicaName: string; replicaHeapLimit: number }) => {
  const { data: objects } = useLargestMaintainedQueries({
    clusterId,
    clusterName,
    limit: TOP_OBJECT_COUNT,
    replicaName,
    replicaHeapLimit,
  });
  return (
    <MemoryByObjectCard
      objects={objects ?? []}
      replicaName={replicaName}
      hasHeapLimit={replicaHeapLimit > 0}
    />
  );
};

const LiveMemoryByObject = ({
  replicaId,
  replicaName,
  replicaHeapLimit,
}: {
  replicaId: string;
  replicaName: string;
  replicaHeapLimit: number;
}) => {
  const { objects, snapshotComplete, isError } = useLiveLargestObjects({
    replicaId,
    heapLimit: replicaHeapLimit,
  });
  if (isError) {
    return <Alert variant="error" message="Object memory is unavailable." />;
  }
  if (!snapshotComplete) return <LoadingContainer />;
  return (
    <MemoryByObjectCard
      objects={objects}
      replicaName={replicaName}
      hasHeapLimit={replicaHeapLimit > 0}
    />
  );
};

const MemoryByObjectCard = ({
  objects,
  replicaName,
  hasHeapLimit,
}: {
  objects: LargestObject[];
  replicaName: string;
  /** Without a heap limit there are no shares, so the card lists sizes alone. */
  hasHeapLimit: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objectLink = useObjectLink();

  if (objects.length === 0) return null;

  // Colored by rank like the Freshness page's lines, so the largest objects
  // are furthest apart in hue.
  const palette = generateRainbowPalette(objects.length);
  const widths = segmentWidths(
    objects.map((object) => object.memoryPercentage),
  );
  const segments = objects.map((object, index) => {
    return {
      key: object.id ?? object.dataflowId ?? String(index),
      label: object.name ?? object.dataflowName ?? "-",
      usage: formatMemoryUsage(hasHeapLimit ? object : { size: object.size }),
      color: palette[index],
      widthPercent: widths[index],
      path: object.isOrphanedDataflow ? undefined : objectLink(object),
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
            {objects.length < TOP_OBJECT_COUNT
              ? "Objects by Memory"
              : `Top ${TOP_OBJECT_COUNT} Objects by Memory`}
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {hasHeapLimit
              ? `% of heap limit on ${replicaName}`
              : `Size on ${replicaName}`}
          </Text>
        </HStack>
        {hasHeapLimit && (
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
        )}
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
