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
  Spinner,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import { TooltipColorSwatch } from "~/components/graphComponents";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { generateRainbowPalette } from "~/components/ThresholdLineGraph/thresholdLineGraphHelpers";
import { InfoIcon } from "~/icons";
import {
  LargestReplica,
  LargestReplicaBoundary,
} from "~/platform/clusters/LargestMaintainedQueries";
import {
  UNIFIED_OBJECT_SIZES_MIN_VERSION,
  useLargestMaintainedQueries,
  useReplicaHeapBytes,
  useReplicaHeapLimits,
  useReplicaSizeAllocations,
} from "~/platform/clusters/queries";
import { useEnvironmentGate } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { formatMemoryUsage } from "~/utils/format";

import {
  LargestObject,
  otherMemory,
  segmentWidths,
  TOP_OBJECT_COUNT,
} from "./memoryByObjectModel";
import { PERCENT_TICKS } from "./ResourceUsage/resourceUsageStyles";
import { useLiveLargestObjects } from "./useLiveLargestObjects";
import { useObjectLink } from "./useObjectLink";

export interface MemoryByObjectProps {
  cluster: Cluster;
  /** The replica to show, else the cluster's largest. */
  replica?: Replica;
}

const OTHER_MEMORY_DESCRIPTION =
  "Heap in use that isn't attributed to the objects listed. Object sizes come from arrangement sizes, which undercount.";

/**
 * Heap limits and use of the cluster's current replicas, from the reads the
 * resource usage card makes with the same keys.
 */
const useReplicaHeaps = (cluster: Cluster) => {
  const replicaIds = cluster.replicas.map(({ id }) => id).sort();
  const sizes = [
    ...new Set(cluster.replicas.flatMap(({ size }) => (size ? [size] : []))),
  ].sort();
  const heapLimits = useReplicaHeapLimits(replicaIds);
  const allocations = useReplicaSizeAllocations(sizes);
  const heapBytes = useReplicaHeapBytes(replicaIds);
  return {
    isLoading:
      heapLimits.isLoading || allocations.isLoading || heapBytes.isLoading,
    /**
     * The replica's heap limit across its processes, as
     * `buildLargestClusterReplicaQuery` computes it. Null if the process counts
     * failed to load, since a per-process limit would inflate every share.
     */
    heapLimitOf: ({ id, size }: Replica) => {
      const perProcess = heapLimits.data?.get(id);
      if (perProcess === undefined || allocations.isError) return null;
      const processes = size
        ? allocations.data?.get(size)?.processes
        : undefined;
      return perProcess * (processes ?? 1);
    },
    heapBytesOf: (replicaId: string) => heapBytes.data?.get(replicaId),
  };
};

/**
 * The largest objects on a replica, as shares of its heap limit: the named
 * replica, else the cluster's largest. A named replica is read from the page's
 * clusters and heap reads, so switching to one waits only on its sizes.
 */
export const MemoryByObject = ({ cluster, replica }: MemoryByObjectProps) => {
  const hasLiveSizes =
    useEnvironmentGate(UNIFIED_OBJECT_SIZES_MIN_VERSION) === true;
  const heaps = useReplicaHeaps(cluster);
  return (
    <LargestReplicaBoundary
      clusterId={cluster.id}
      replica={
        replica && {
          id: replica.id,
          name: replica.name,
          heapLimit: heaps.heapLimitOf(replica),
        }
      }
      wrapFallback={(fallback) => (
        <MemoryByObjectFrame>{fallback}</MemoryByObjectFrame>
      )}
    >
      {(shown) =>
        hasLiveSizes ? (
          <LiveMemoryByObject
            replica={shown}
            heapUsedBytes={heaps.heapBytesOf(shown.id)}
            isHeapLoading={heaps.isLoading}
          />
        ) : heaps.isLoading ? (
          // A named replica's polled sizes are keyed on its heap limit, so
          // loading them before it would load them twice.
          <MemoryByObjectFrame>
            <LoadingContainer />
          </MemoryByObjectFrame>
        ) : (
          <PolledMemoryByObject
            clusterId={cluster.id}
            clusterName={cluster.name}
            replica={shown}
            heapUsedBytes={heaps.heapBytesOf(shown.id)}
          />
        )
      }
    </LargestReplicaBoundary>
  );
};

const PolledMemoryByObject = ({
  clusterId,
  clusterName,
  replica,
  heapUsedBytes,
}: {
  clusterId: string;
  clusterName: string;
  replica: LargestReplica;
  heapUsedBytes: number | undefined;
}) => {
  const { data: objects } = useLargestMaintainedQueries({
    clusterId,
    clusterName,
    limit: TOP_OBJECT_COUNT,
    replicaName: replica.name,
    replicaHeapLimit: replica.heapLimit,
  });
  return (
    <MemoryByObjectCard
      objects={objects ?? []}
      replicaName={replica.name}
      heapLimit={replica.heapLimit}
      heapUsedBytes={heapUsedBytes}
    />
  );
};

/** `value`, or while it's undefined, the last value it had in a commit. */
function useLastDefined<T>(value: T | undefined) {
  const last = React.useRef(value);
  React.useEffect(() => {
    if (value !== undefined) last.current = value;
  });
  return value ?? last.current;
}

const LiveMemoryByObject = ({
  replica,
  heapUsedBytes,
  isHeapLoading,
}: {
  replica: LargestReplica;
  heapUsedBytes: number | undefined;
  isHeapLoading: boolean;
}) => {
  const { objects, snapshotComplete, isRefreshing, isError } =
    useLiveLargestObjects({
      replicaId: replica.id,
      heapLimit: replica.heapLimit,
    });
  const content: MemoryByObjectContent | undefined =
    snapshotComplete && !isHeapLoading
      ? {
          objects,
          replicaName: replica.name,
          heapLimit: replica.heapLimit,
          heapUsedBytes,
        }
      : undefined;
  // A switch to another replica keeps the previous one's content on screen
  // until the new one's first snapshot and heap reads are in.
  const shown = useLastDefined(content);
  if (isError) {
    return (
      <MemoryByObjectFrame>
        <Alert variant="error" message="Object memory is unavailable." />
      </MemoryByObjectFrame>
    );
  }
  if (!shown) {
    return (
      <MemoryByObjectFrame>
        <LoadingContainer />
      </MemoryByObjectFrame>
    );
  }
  return (
    <MemoryByObjectCard
      {...shown}
      isRefreshing={isRefreshing || !content}
      isStale={!content}
    />
  );
};

/** The card's shell, which also holds its loading, error and empty states. */
const MemoryByObjectFrame = ({
  title = "Objects by Memory",
  subtitle,
  isRefreshing = false,
  isStale = false,
  children,
}: {
  title?: string;
  subtitle?: string;
  isRefreshing?: boolean;
  /** Dims content kept on screen while the selected replica's loads. */
  isStale?: boolean;
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      borderWidth="1px"
      borderColor={colors.border.primary}
      aria-busy={isRefreshing}
    >
      <VStack spacing={4} alignItems="stretch">
        <HStack spacing={2} alignItems="baseline" flexWrap="wrap">
          <Text as="h3" textStyle="heading-sm">
            {title}
          </Text>
          {subtitle && (
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              opacity={isStale ? 0.5 : undefined}
            >
              {subtitle}
            </Text>
          )}
          {isRefreshing && (
            <Spinner
              size="sm"
              alignSelf="center"
              ml="auto"
              data-testid="refreshing-spinner"
            />
          )}
        </HStack>
        <VStack
          spacing={4}
          alignItems="stretch"
          opacity={isStale ? 0.5 : undefined}
        >
          {children}
        </VStack>
      </VStack>
    </Card>
  );
};

interface Segment {
  key: string;
  label: string;
  usage: string;
  color: string;
  widthPercent: number;
  path?: string;
  description?: string;
}

interface MemoryByObjectContent {
  objects: LargestObject[];
  replicaName: string;
  /** Without a heap limit there are no shares, so the card lists sizes alone. */
  heapLimit: number | null;
  /** The replica's heap in use, which sizes the other memory segment. */
  heapUsedBytes: number | undefined;
}

const MemoryByObjectCard = ({
  objects,
  replicaName,
  heapLimit,
  heapUsedBytes,
  isRefreshing,
  isStale,
}: MemoryByObjectContent & {
  isRefreshing?: boolean;
  isStale?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const objectLink = useObjectLink();

  const hasHeapLimit = heapLimit !== null;
  if (objects.length === 0) {
    return (
      <MemoryByObjectFrame isRefreshing={isRefreshing} isStale={isStale}>
        <Text textStyle="text-base" color={colors.foreground.secondary}>
          No objects are using memory on {replicaName}.
        </Text>
      </MemoryByObjectFrame>
    );
  }

  // Colored by rank like the Freshness page's lines, so the largest objects
  // are furthest apart in hue.
  const palette = generateRainbowPalette(objects.length);
  const other =
    heapLimit && heapUsedBytes !== undefined
      ? otherMemory(heapUsedBytes, heapLimit, objects)
      : undefined;
  // Other memory goes last, so the end of the bar clips it before any object.
  const widths = segmentWidths(
    [...objects, ...(other ? [other] : [])].map(
      ({ memoryPercentage }) => memoryPercentage,
    ),
  );
  const segments: Segment[] = objects.map((object, index) => {
    return {
      key: object.id ?? object.dataflowId ?? String(index),
      label: object.name ?? object.dataflowName ?? "-",
      usage: formatMemoryUsage(hasHeapLimit ? object : { size: object.size }),
      color: palette[index],
      widthPercent: widths[index],
      path: object.isOrphanedDataflow ? undefined : objectLink(object),
    };
  });
  if (other) {
    segments.push({
      key: "other-memory",
      label: "Other memory",
      usage: formatMemoryUsage(other),
      color: colors.foreground.tertiary,
      widthPercent: widths[objects.length],
      description: OTHER_MEMORY_DESCRIPTION,
    });
  }

  return (
    <MemoryByObjectFrame
      title={
        objects.length < TOP_OBJECT_COUNT
          ? "Objects by Memory"
          : `Top ${TOP_OBJECT_COUNT} Objects by Memory`
      }
      subtitle={
        hasHeapLimit
          ? `% of heap limit on ${replicaName}`
          : `Size on ${replicaName}`
      }
      isRefreshing={isRefreshing}
      isStale={isStale}
    >
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
                    label={
                      <>
                        <Text>{`${segment.label} · ${segment.usage}`}</Text>
                        {segment.description && (
                          <Text>{segment.description}</Text>
                        )}
                      </>
                    }
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
            {segment.description && (
              <Tooltip label={segment.description}>
                <InfoIcon aria-label={segment.description} />
              </Tooltip>
            )}
          </HStack>
        ))}
      </Flex>
    </MemoryByObjectFrame>
  );
};
