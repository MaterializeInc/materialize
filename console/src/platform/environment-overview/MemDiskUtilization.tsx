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
  Button,
  HStack,
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverContent,
  PopoverHeader,
  PopoverTrigger,
  Portal,
  Stack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { Gauge } from "~/components/Gauge";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { TIME_PERIOD_SEARCH_PARAM_KEY } from "~/hooks/useTimePeriodSelect";
import { ClustersIcon } from "~/icons";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { notNullOrUndefined } from "~/util";
import { formatDate } from "~/utils/dateFormat";

import { useClusterMemDiskUtilization } from "./queries";
import { MemDiskUtilizationStatus, ThresholdPercentages } from "./utils";

const UTILIZATION_STATUS_TO_DESCRIPTION = {
  optimal: "The cluster is performing optimally.",
  suboptimal: "Latency is negatively impacted due to memory spilling to disk.",
  underProvisioned:
    "The cluster is at risk of crash looping due to running out of memory and disk. Consider increasing the size of your cluster.",
};

const utilizationStatusToColor = (
  utilizationStatus: MemDiskUtilizationStatus,
  colors: MaterializeTheme["colors"],
) => {
  switch (utilizationStatus) {
    case "empty":
      return colors.foreground.tertiary;
    case "optimal":
      return colors.green[400];
    case "suboptimal":
      return colors.yellow[600];
    case "underProvisioned":
      return colors.accent.red;
  }
};

const formatPercentage = (percentage: number) =>
  `${(percentage * 100).toFixed(2)}%`;

const buildReplicaTickMarks = ({
  thresholdPercentages,
  colors,
}: {
  thresholdPercentages: ThresholdPercentages;
  colors: MaterializeTheme["colors"];
}) => {
  return [
    {
      color: utilizationStatusToColor("optimal", colors),
      startPercentage: 0,
      endPercentage: thresholdPercentages.optimal,
    },
    {
      color: utilizationStatusToColor("suboptimal", colors),
      startPercentage: thresholdPercentages.optimal,
      endPercentage: thresholdPercentages.suboptimal,
    },
    {
      color: utilizationStatusToColor("underProvisioned", colors),
      startPercentage: thresholdPercentages.suboptimal,
      endPercentage: 1,
    },
  ];
};

const MemDiskUtilizationDetailItem = ({
  label,
  children,
}: {
  label: React.ReactNode;
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack gap="2" width="100%" justifyContent="space-between">
      <Text
        textStyle="text-base"
        color={colors.foreground.secondary}
        whiteSpace="nowrap"
      >
        {label}
      </Text>
      <Text textStyle="text-base" color={colors.foreground.primary}>
        {children}
      </Text>
    </HStack>
  );
};

const MemDiskUtilizationCardBody = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: utilizationByClusterId } = useClusterMemDiskUtilization();
  const regionSlug = useRegionSlug();

  if (utilizationByClusterId.size === 0) {
    return (
      <EmptyListWrapper padding="4">
        <EmptyListHeader>
          <ClustersIcon />
          <EmptyListHeaderContents title="No available clusters" />
        </EmptyListHeader>
      </EmptyListWrapper>
    );
  }

  const utilizationSortedByClusterName = [
    ...(utilizationByClusterId?.values() ?? []),
  ].sort((clusterA, clusterB) =>
    clusterA.clusterName.localeCompare(clusterB.clusterName),
  );

  return (
    <Stack width="100%" direction="row" flexWrap="wrap" padding="4" spacing="4">
      {utilizationSortedByClusterName.map((utilization) => {
        const buckets = [...utilization.buckets.entries()].sort(
          ([timestampA], [timestampB]) => {
            return timestampA - timestampB;
          },
        );
        const clusterHasDisk = utilization.replicas.some((r) => r.disk);
        const replicaSizes = utilization?.replicas
          .map((replica) => replica.size)
          .filter(notNullOrUndefined);
        return (
          <VStack
            key={utilization.clusterId}
            alignItems="flex-start"
            width="418px"
          >
            <HStack width="100%">
              <HStack spacing="1" flexGrow="1" minWidth="0">
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {utilization.clusterName}
                </Text>

                {replicaSizes.length > 0 && (
                  <Text
                    textStyle="text-base"
                    color={colors.foreground.secondary}
                    as="span"
                  >
                    ({replicaSizes.join(", ")})
                  </Text>
                )}
              </HStack>

              <Button
                as={Link}
                to={{
                  pathname: absoluteClusterPath(regionSlug, {
                    id: utilization.clusterId,
                    name: utilization.clusterName,
                  }),
                  search: `${TIME_PERIOD_SEARCH_PARAM_KEY}=${20160}`,
                }}
                background={colors.background.primary}
                size="xs"
                variant="borderless"
                fontSize="12px"
                paddingX="2"
                mr="-2"
              >
                View cluster
              </Button>
            </HStack>

            <HStack spacing="0.5" width="100%">
              {buckets.map(([bucketTs, bucket]) => {
                return (
                  <Popover trigger="hover" key={bucketTs} isLazy>
                    <PopoverTrigger>
                      <Box
                        height="4"
                        flexShrink="0"
                        width="2"
                        borderRadius="2px"
                        cursor="pointer"
                        backgroundColor={utilizationStatusToColor(
                          bucket.status,
                          colors,
                        )}
                        _hover={{
                          filter: "brightness(0.8)",
                        }}
                      />
                    </PopoverTrigger>

                    <Portal>
                      <PopoverContent maxWidth="400px">
                        <PopoverArrow />
                        <PopoverHeader>
                          <Text>
                            <Text as="span" textStyle="text-ui-med">
                              {formatDate(bucket.bucketStart, `MMM do,	HH:mm`)}
                            </Text>{" "}
                            -{" "}
                            <Text as="span" textStyle="text-ui-med">
                              {formatDate(bucket.bucketEnd, `HH:mm z`)}
                            </Text>
                            {bucket.status !== "empty" && (
                              <Text
                                as="span"
                                textStyle="text-ui-med"
                                color={colors.foreground.secondary}
                              >
                                {" "}
                                ({formatDate(bucket.occurredAt, "HH:mm z")})
                              </Text>
                            )}
                          </Text>
                        </PopoverHeader>
                        <PopoverBody>
                          {bucket.status === "empty" ? (
                            <Text textStyle="text-small">
                              No data available.
                            </Text>
                          ) : bucket.oomEvents.length > 0 ? (
                            <>
                              <Text textStyle="text-ui-sm">
                                The cluster ran out of memory
                                {bucket.oomEvents.length > 1
                                  ? ` ${bucket.oomEvents.length} times`
                                  : ""}
                                . Consider increasing the size of your cluster.
                              </Text>
                              {bucket.replicaSize && (
                                <Text textStyle="text-ui-sm">
                                  <Text
                                    as="span"
                                    color={colors.foreground.secondary}
                                  >
                                    Size:
                                  </Text>{" "}
                                  {bucket.replicaSize}
                                </Text>
                              )}
                            </>
                          ) : (
                            <>
                              {UTILIZATION_STATUS_TO_DESCRIPTION[
                                bucket.status
                              ] && (
                                <Text
                                  textStyle="text-small"
                                  color={colors.foreground.secondary}
                                >
                                  {
                                    UTILIZATION_STATUS_TO_DESCRIPTION[
                                      bucket.status
                                    ]
                                  }
                                </Text>
                              )}

                              <HStack px="2" width="100%" spacing="8">
                                <Gauge
                                  containerProps={{
                                    flexShrink: 0,
                                    mt: "4",
                                  }}
                                  percentage={
                                    bucket.peakMemDiskUtilizationPercent ?? 0
                                  }
                                  size={80}
                                  tickMarks={buildReplicaTickMarks({
                                    thresholdPercentages:
                                      bucket.thresholdPercents,
                                    colors,
                                  })}
                                />
                                <VStack width="100%" spacing="0">
                                  <MemDiskUtilizationDetailItem label="Memory Utilization">
                                    {bucket.heapPercent
                                      ? formatPercentage(bucket.heapPercent)
                                      : "-"}
                                  </MemDiskUtilizationDetailItem>
                                  {clusterHasDisk && (
                                    <MemDiskUtilizationDetailItem label="Disk">
                                      {bucket.diskPercent
                                        ? formatPercentage(bucket.diskPercent)
                                        : "-"}
                                    </MemDiskUtilizationDetailItem>
                                  )}
                                  {bucket.replicaSize && (
                                    <MemDiskUtilizationDetailItem label="Size">
                                      {bucket.replicaSize}
                                    </MemDiskUtilizationDetailItem>
                                  )}
                                </VStack>
                              </HStack>
                            </>
                          )}
                        </PopoverBody>
                      </PopoverContent>
                    </Portal>
                  </Popover>
                );
              })}
            </HStack>
          </VStack>
        );
      })}
    </Stack>
  );
};

export const MemDiskUtilization = () => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      borderColor={colors.border.primary}
      borderRadius="lg"
      borderWidth="1px"
      spacing="0"
    >
      <VStack
        alignItems="flex-start"
        spacing="0"
        width="100%"
        padding="4"
        borderColor={colors.border.primary}
        borderBottomWidth="1px"
      >
        <VStack spacing="1" alignItems="flex-start">
          <Text textStyle="heading-md" color={colors.foreground.primary}>
            Cluster utilization
          </Text>

          <Text textStyle="text-small" color={colors.foreground.secondary}>
            The status for each cluster in your environment based on its
            resource utilization over the last 14 days. Use the status bars
            below as guidance to{" "}
            <TextLink
              href={`${docUrls["/docs/concepts/clusters/"]}#sizing-your-clusters`}
              isExternal
            >
              size your clusters
            </TextLink>
            .
          </Text>
        </VStack>
      </VStack>
      <AppErrorBoundary
        message="An error occurred fetching cluster utilization data."
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
          <MemDiskUtilizationCardBody />
        </React.Suspense>
      </AppErrorBoundary>
    </VStack>
  );
};

export default MemDiskUtilization;
