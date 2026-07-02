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
  HStack,
  SimpleGrid,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { Bucket } from "~/api/materialize/cluster/replicaUtilizationHistory";
import TextLink from "~/components/TextLink";
import {
  calculateMemDiskUtilizationStatus,
  DEFAULT_THRESHOLD_PERCENTAGES,
  utilizationStatusToColor,
} from "~/platform/environment-overview/utils";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";
import WarningIcon from "~/svg/WarningIcon";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDate } from "~/utils/dateFormat";
import { formatPercentage } from "~/utils/format";

import { RenderableNode } from "./criticalPathRenderable";
import { ReplicaInWindow, useClusterBucketsInWindow } from "./queries";
import { useJourneyLinkState } from "./useJourneyLinkState";

type Cluster = NonNullable<RenderableNode["cluster"]>;

const MetricCard = ({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      px={3}
      py={2}
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="md"
    >
      <VStack align="stretch" spacing={2}>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {label}
        </Text>
        {children}
      </VStack>
    </Card>
  );
};

const MetricBarCard = ({
  label,
  fraction,
}: {
  label: string;
  fraction: number | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const status = calculateMemDiskUtilizationStatus({
    thresholdPercentages: DEFAULT_THRESHOLD_PERCENTAGES,
    peakMemDiskUtilizationPercent: fraction,
  });
  const isCritical = status === "underProvisioned";
  const barColor = utilizationStatusToColor(status, colors);
  const widthPct = fraction === null ? 0 : Math.min(1, fraction) * 100;
  return (
    <MetricCard label={label}>
      <HStack spacing={1} align="center">
        {isCritical && (
          <WarningIcon
            boxSize="3"
            color={colors.accent.red}
            aria-label="critical"
          />
        )}
        <Text
          textStyle="text-ui-med"
          color={isCritical ? colors.accent.red : undefined}
        >
          {fraction === null ? "—" : formatPercentage(fraction)}
        </Text>
      </HStack>
      <Box
        width="100%"
        height="2"
        borderRadius="full"
        bg={colors.background.secondary}
        overflow="hidden"
      >
        <Box
          height="100%"
          width={`${widthPct}%`}
          borderRadius="full"
          bg={barColor}
          transition="width 0.3s ease"
        />
      </Box>
    </MetricCard>
  );
};

const EmptyStateCard = ({ children }: { children: React.ReactNode }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      px={3}
      py={3}
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="md"
    >
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {children}
      </Text>
    </Card>
  );
};

const ClusterMetricsHeader = ({
  node,
  pageObjectId,
  timestamp,
}: {
  node: RenderableNode;
  pageObjectId: string;
  timestamp: Date | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const journeyLinkState = useJourneyLinkState(pageObjectId);
  const objectLinkTarget = `../${node.parentSourceId ?? node.id}`;
  return (
    <VStack align="stretch" spacing={1}>
      <Text textStyle="heading-sm" noOfLines={1} title={node.name}>
        Cluster metrics for{" "}
        {node.isProbe ? (
          node.name
        ) : (
          <TextLink
            as={RouterLink}
            to={objectLinkTarget}
            relative="path"
            state={journeyLinkState}
          >
            {node.name}
          </TextLink>
        )}
        {timestamp && (
          <Text as="span" color={colors.foreground.secondary} ml={2}>
            at {formatDate(timestamp, "h:mm:ss a")}
          </Text>
        )}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Memory and CPU on the replica hosting the node you selected above.
      </Text>
    </VStack>
  );
};

const ClusterMetricCard = ({ cluster }: { cluster: Cluster }) => {
  const regionSlug = useRegionSlug();
  return (
    <MetricCard label="Cluster">
      <TextLink
        as={RouterLink}
        to={absoluteClusterPath(regionSlug, cluster)}
        textStyle="text-ui-med"
        noOfLines={1}
      >
        {cluster.name}
      </TextLink>
    </MetricCard>
  );
};

const ReplicaInfoCard = ({
  replica,
  isDropped,
}: {
  replica: Bucket;
  isDropped: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <MetricCard label="Replica">
      <HStack spacing={1} flexWrap="wrap">
        <Text textStyle="text-ui-med">{replica.name}</Text>
        {replica.size && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            ({replica.size})
          </Text>
        )}
        {isDropped && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            (dropped)
          </Text>
        )}
      </HStack>
    </MetricCard>
  );
};

const ReplicaMetricsRow = ({
  cluster,
  replica,
  isDropped,
}: {
  cluster: Cluster;
  replica: Bucket;
  isDropped: boolean;
}) => (
  <SimpleGrid columns={2} spacing={3}>
    <ClusterMetricCard cluster={cluster} />
    <ReplicaInfoCard replica={replica} isDropped={isDropped} />
    {/* Use maxHeap to match the "Memory Utilization" graph on the cluster
        overview. heap_percent already falls back to size-based memory percent
        when heap_limit is null (self-managed/emulator), so this is correct in
        both environments. */}
    <MetricBarCard label="Memory" fraction={replica.maxHeap.percent} />
    <MetricBarCard label="CPU" fraction={replica.maxCpu.percent} />
  </SimpleGrid>
);

const ReplicaMetricsList = ({
  cluster,
  replicas,
  isLoading,
  isError,
}: {
  cluster: Cluster | null;
  replicas: Bucket[] | undefined;
  isLoading: boolean;
  isError: boolean;
}) => {
  const { getClusterById } = useAllClusters();
  if (cluster === null) {
    return (
      <EmptyStateCard>This object does not run on a cluster.</EmptyStateCard>
    );
  }
  if (isLoading) return <Spinner size="sm" />;
  if (isError) {
    return <EmptyStateCard>Failed to load cluster metrics.</EmptyStateCard>;
  }
  if (!replicas || replicas.length === 0) {
    return <EmptyStateCard>Inactive — replication factor 0</EmptyStateCard>;
  }
  const liveReplicaIds = new Set(
    getClusterById(cluster.id)?.replicas.map((r) => r.id) ?? [],
  );
  return (
    <VStack align="stretch" spacing={3}>
      {replicas.map((replica) => (
        <ReplicaMetricsRow
          key={replica.replicaId}
          cluster={cluster}
          replica={replica}
          isDropped={!liveReplicaIds.has(replica.replicaId)}
        />
      ))}
    </VStack>
  );
};

const DroppedReplicasFooter = ({
  windowReplicas,
  lockedReplicas,
}: {
  windowReplicas: ReplicaInWindow[];
  lockedReplicas: Bucket[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const lockedIds = new Set(lockedReplicas.map((r) => r.replicaId));
  const dropped = windowReplicas.filter((r) => !lockedIds.has(r.replicaId));
  if (dropped.length === 0) return null;
  return (
    <Card px={3} py={2} bg={colors.background.secondary} borderRadius="md">
      <HStack spacing={2} flexWrap="wrap">
        <Text textStyle="text-small">
          {dropped.length} {pluralize(dropped.length, "replica", "replicas")}{" "}
          dropped in this window:
        </Text>
        {dropped.map((r, i) => (
          <Text
            key={r.replicaId}
            textStyle="text-small"
            color={colors.foreground.secondary}
          >
            {r.name}
            {r.size && ` (${r.size})`} · Last seen{" "}
            {formatDate(r.lastSeenAt, "h:mm:ss a")}
            {i < dropped.length - 1 && " ·"}
          </Text>
        ))}
      </HStack>
    </Card>
  );
};

export interface CriticalPathClusterMetricsProps {
  node: RenderableNode;
  pageObjectId: string;
  timestamp: Date | null;
  bucketSizeMs: number;
  lookbackMs: number;
}

export const CriticalPathClusterMetrics = ({
  node,
  pageObjectId,
  timestamp,
  bucketSizeMs,
  lookbackMs,
}: CriticalPathClusterMetricsProps) => {
  const clusterId = node.cluster?.id ?? null;
  const {
    data: bucketsByReplicaId,
    isLoading,
    isError,
  } = useClusterBucketsInWindow({
    clusterId,
    lookbackMs,
    bucketSizeMs,
    anchorTimestamp: timestamp,
  });

  const targetBucketStartMs =
    Math.floor((timestamp ?? new Date()).getTime() / bucketSizeMs) *
    bucketSizeMs;
  const buckets = Object.values(bucketsByReplicaId ?? {});
  const replicas = buckets.flatMap((bs) =>
    bs.filter((b) => b.bucketStart.getTime() === targetBucketStartMs),
  );
  const replicasInWindow: ReplicaInWindow[] = buckets
    .flatMap((bs) => bs.slice(-1))
    .map(({ replicaId, name, size, bucketEnd }) => ({
      replicaId,
      name,
      size,
      lastSeenAt: bucketEnd,
    }));

  return (
    <VStack align="stretch" spacing={3} width="100%">
      <ClusterMetricsHeader
        node={node}
        pageObjectId={pageObjectId}
        timestamp={timestamp}
      />
      <ReplicaMetricsList
        cluster={node.cluster}
        replicas={replicas}
        isLoading={isLoading}
        isError={isError}
      />
      <DroppedReplicasFooter
        windowReplicas={replicasInWindow}
        lockedReplicas={replicas}
      />
    </VStack>
  );
};
