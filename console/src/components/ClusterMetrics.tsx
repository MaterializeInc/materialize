// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  BoxProps,
  Button,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link } from "react-router-dom";

import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useClusterReplicaMetrics } from "~/queries/clusterReplicaMetrics";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";
import BoxIcon from "~/svg/BoxIcon";
import { ClustersIcon } from "~/svg/nav/ClustersIcon";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatBytesShort } from "~/utils/format";

import Alert from "./Alert";
import { AppErrorBoundary } from "./AppErrorBoundary";
import { CardFooter } from "./cardComponents";
import { RadialPercentageGraph } from "./RadialPercentageGraph";

const ClusterMetricBox = (props: BoxProps) => {
  return (
    <HStack
      px="6"
      py="4"
      alignItems="center"
      justifyContent="center"
      width="100%"
      gap={2}
      {...props}
    >
      {props.children}
    </HStack>
  );
};

export interface ClusterMetricsProps extends BoxProps {
  clusterId: string;
  clusterName: string;
  replicaName?: string;
}

export const ClusterMetricsInner = ({
  clusterId,
  clusterName,
  replicaName,
  ...props
}: ClusterMetricsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();

  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);

  const { data } = useClusterReplicaMetrics({
    clusterId,
  });
  const clusterHasDisk =
    Boolean(cluster?.disk) || Boolean(cluster?.replicas.some((r) => r.disk));
  const replicaCount = data.rows.length;
  const metrics = data.rows[0];
  if (replicaCount === 0) {
    return (
      <VStack
        alignItems="flex-start"
        justifyContent="flex-start"
        width="100%"
        {...props}
      >
        <Alert
          variant="info"
          showLabel={false}
          label="Cluster paused"
          message={
            <Text>
              <Link
                to={absoluteClusterPath(regionSlug, {
                  id: clusterId,
                  name: clusterName,
                })}
              >
                <Text
                  as="span"
                  textStyle="text-ui-med"
                  textDecoration="underline"
                >
                  {clusterName}
                </Text>
              </Link>{" "}
              cluster is currently inactive
            </Text>
          }
          w="100%"
        />
      </VStack>
    );
  }

  // To support decimal results safely, we divide by 1 million first, then convert to a
  // number and divide by the remaining 1000.
  const cores = Number(metrics.cpuNanoCores / 1_000_000n) / 1000.0;

  const metricsUnavailable =
    !metrics.memoryPercent &&
    !metrics.cpuPercent &&
    !metrics.heapPercent &&
    (!clusterHasDisk || !metrics.diskPercent);

  return (
    <VStack
      gap="0"
      width="100%"
      borderColor={colors.border.primary}
      borderRadius="8px"
      borderWidth="1px"
      overflow="hidden"
      {...props}
    >
      <HStack
        borderBottomWidth="1px"
        borderColor={colors.border.primary}
        gap="0"
        justifyContent="space-between"
        width="100%"
      >
        <ClusterMetricBox
          borderColor={colors.border.secondary}
          borderRightWidth="1px"
          aria-label="Number of replicas"
        >
          <Flex alignItems="center" justifyContent="center" w={4} h={4}>
            <ClustersIcon aria-hidden="true" />
          </Flex>
          <Text textStyle="text-ui-med">
            {replicaCount}{" "}
            <Text
              as="span"
              color={colors.foreground.secondary}
              textStyle="text-ui-reg"
            >
              {pluralize(replicaCount, "Replica", "Replicas")}
            </Text>
          </Text>
        </ClusterMetricBox>
        <ClusterMetricBox aria-label="Replica size">
          <Flex alignItems="center" justifyContent="center" w={4} h={4}>
            <BoxIcon aria-hidden="true" />
          </Flex>
          <Text color={colors.foreground.primary} textStyle="text-ui-med">
            {metrics.size}
          </Text>
        </ClusterMetricBox>
      </HStack>
      <HStack justifyContent="space-evenly" py="6" px="4" width="100%">
        {metricsUnavailable && (
          <VStack
            width="100%"
            height="100%"
            alignItems="center"
            justifyContent="center"
            gap={4}
            data-testid="waiting-for-metrics"
          >
            <Spinner
              size="md"
              thickness="1.5px"
              speed="0.65s"
              color={colors.foreground.primary}
              data-testid="loading-spinner"
            />
            <VStack spacing={0}>
              <Text color={colors.foreground.primary} textStyle="text-ui-med">
                Cluster is warming up
              </Text>
              <Text color={colors.foreground.secondary} textStyle="text-small">
                Metrics will be available shortly
              </Text>
            </VStack>
          </VStack>
        )}
        {metrics.heapPercent && (
          <RadialPercentageGraph percentage={metrics.heapPercent ?? 0}>
            <Text textStyle="text-ui-med">Memory Utilization</Text>
            <Text
              color={colors.foreground.secondary}
              textAlign="center"
              textStyle="text-small"
            >
              {formatBytesShort(metrics.heapBytes)}
            </Text>
          </RadialPercentageGraph>
        )}
        {metrics.cpuPercent && (
          <RadialPercentageGraph percentage={metrics.cpuPercent}>
            <Text textStyle="text-ui-med">CPU</Text>
            <Text
              color={colors.foreground.secondary}
              textAlign="center"
              textStyle="text-small"
            >
              {cores.toString()} cores
            </Text>
          </RadialPercentageGraph>
        )}
        {clusterHasDisk && metrics.diskPercent && (
          <RadialPercentageGraph percentage={metrics.diskPercent}>
            <Text textStyle="text-ui-med">Disk</Text>
            <Text
              color={colors.foreground.secondary}
              textAlign="center"
              textStyle="text-small"
            >
              {formatBytesShort(metrics.diskBytes)}
            </Text>
          </RadialPercentageGraph>
        )}
      </HStack>
      <CardFooter>
        <Text wordBreak="break-all" textStyle="text-small" noOfLines={1}>
          <Text as="span" color={colors.foreground.secondary}>
            Usage metrics from
          </Text>{" "}
          <Text as="span" fontWeight={500}>
            {clusterName}
            {replicaName ? `.${replicaName}` : ""}
          </Text>
        </Text>
        <Button
          as={Link}
          to={absoluteClusterPath(regionSlug, {
            id: clusterId,
            name: clusterName,
          })}
          background={colors.background.primary}
          size="sm"
          variant="outline"
        >
          View cluster
        </Button>
      </CardFooter>
    </VStack>
  );
};

export const ClusterMetrics = (props: ClusterMetricsProps) => {
  return (
    <AppErrorBoundary message="An error occurred loading cluster metrics.">
      <ClusterMetricsInner {...props} />
    </AppErrorBoundary>
  );
};
