// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Card, Code, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { IPostgresInterval } from "~/api/materialize";
import {
  fetchShowCreate,
  ShowCreateObjectType,
} from "~/api/materialize/showCreate";
import { fetchSourceStatistics } from "~/api/materialize/source/sourceStatistics";
import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { useQuery } from "@tanstack/react-query";
import Alert from "~/components/Alert";
import { MaterializeTheme } from "~/theme";
import { formatBytesShort } from "~/utils/format";
import { sumPostgresIntervalMs } from "~/util";

import { DependenciesSection } from "./DependenciesSection";
import { ObjectDetailsCard } from "./ObjectDetailsCard";
import { ObjectFreshnessChart } from "./ObjectFreshnessChart";
import { ObjectMemoryCard } from "./ObjectMemoryCard";
import { useAllObjectSizes, useObjectColumns, useObjectDetail, useObjectLag, useObjectMemory, useObjectMemoryUnified } from "./queries";
import { MaintainedObjectListRow } from "./types";

export interface ObjectDetailPanelProps {
  object: MaintainedObjectListRow;
  breadcrumbs?: { id: string; name: string }[];
  onObjectClick?: (id: string) => void;
  onBreadcrumbClick?: (id: string, index: number) => void;
}

export const ObjectDetailPanel = ({
  object,
  breadcrumbs = [],
  onObjectClick,
  onBreadcrumbClick,
}: ObjectDetailPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const navigate = useNavigate();

  const handleObjectClick = (id: string) => {
    if (onObjectClick) {
      onObjectClick(id);
    } else {
      navigate(`../${id}`, { relative: "path" });
    }
  };

  const {
    data: detail,
    isError: detailError,
  } = useObjectDetail(object.id);

  const { data: liveLag } = useObjectLag(object.id);

  const { data: memory, isLoading: memoryLoading, isFetching: memoryFetching } = useObjectMemory({
    objectId: object.id,
    clusterName: detail?.clusterName ?? undefined,
    replicaName: detail?.replicaName ?? undefined,
    objectType: detail?.objectType,
  });

  const { data: memoryUnified, isLoading: memoryUnifiedLoading } = useObjectMemoryUnified({
    objectId: object.id,
    objectType: object.objectType,
  });

  const { data: allSizes } = useAllObjectSizes();
  const objectCost = allSizes?.get(object.id);

  const isComputeObject =
    object.objectType === "index" || object.objectType === "materialized-view";

  const { data: columns } = useObjectColumns(object.id);

  return (
    <Box p={4}>
      <VStack align="start" spacing={6} width="100%">
        {breadcrumbs.length > 0 && (
          <HStack spacing={1} flexWrap="wrap">
            {breadcrumbs.map((crumb, i) => (
              <React.Fragment key={crumb.id}>
                <Text
                  textStyle="text-small"
                  color={colors.accent.brightPurple}
                  cursor="pointer"
                  _hover={{ textDecoration: "underline" }}
                  onClick={() => onBreadcrumbClick?.(crumb.id, i)}
                  noOfLines={1}
                  maxWidth="200px"
                >
                  {crumb.name}
                </Text>
                <Text textStyle="text-small" color={colors.foreground.secondary}>
                  /
                </Text>
              </React.Fragment>
            ))}
            <Text
              textStyle="text-small"
              color={colors.foreground.primary}
              fontWeight={600}
              noOfLines={1}
              maxWidth="200px"
            >
              {object.name}
            </Text>
          </HStack>
        )}

        {detailError && (
          <Alert
            variant="error"
            message="Failed to load object details. Please try refreshing the page."
            width="100%"
          />
        )}

        <ObjectDetailsCard
          object={{
            ...(detail ? { ...object, ...detail } : object),
            lag: liveLag?.lag ?? object.lag,
            lagMs: liveLag?.lagMs ?? object.lagMs,
          }}
          replicaName={detail?.replicaName ?? null}
          replicaSize={detail?.replicaSize ?? null}
          clusterManaged={detail?.clusterManaged ?? null}
          memoryBytes={memory?.memoryBytes ?? null}
          replicaTotalMemoryBytes={detail?.replicaTotalMemoryBytes ?? null}
        />

        {isComputeObject && (
          <ObjectMemoryCard
            memoryBytes={memory?.memoryBytes ?? null}
            replicaTotalMemoryBytes={detail?.replicaTotalMemoryBytes ?? null}
            replicaName={detail?.replicaName ?? null}
            replicaSize={detail?.replicaSize ?? null}
            isLoading={memoryLoading || memoryFetching}
          />
        )}

        {isComputeObject && (
          <UnifiedMemoryCard
            replicas={memoryUnified ?? null}
            isLoading={memoryUnifiedLoading}
            estimatedCreditsPerHour={objectCost?.estimatedCreditsPerHour ?? null}
            replicaCreditsPerHour={objectCost?.creditsPerHour ?? null}
          />
        )}

        <ObjectFreshnessChart objectId={object.id} />

        <DependenciesSection
          objectId={object.id}
          objectType={object.objectType}
          lagMs={liveLag?.lagMs ?? object.lagMs}
          onObjectClick={handleObjectClick}
        />

        {object.objectType === "source" && (
          <SourceDiagnostics sourceId={object.id} />
        )}

        {columns && columns.length > 0 && (
          <Box width="100%">
            <Text textStyle="heading-sm" mb={3}>
              Columns
            </Text>
            <Box
              borderRadius="md"
              border="1px"
              borderColor={colors.border.primary}
              overflow="hidden"
            >
              <Box as="table" width="100%">
                <Box as="thead" bg={colors.background.secondary}>
                  <Box as="tr">
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Column name</Text>
                    </Box>
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Type</Text>
                    </Box>
                    <Box as="th" px={4} py={2} textAlign="left">
                      <Text textStyle="text-small-heavy">Nullable</Text>
                    </Box>
                  </Box>
                </Box>
                <Box as="tbody">
                  {columns.map((col) => (
                    <Box
                      as="tr"
                      key={col.name}
                      borderTop="1px"
                      borderColor={colors.border.primary}
                    >
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-med">{col.name}</Text>
                      </Box>
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-reg">{col.type}</Text>
                      </Box>
                      <Box as="td" px={4} py={2}>
                        <Text textStyle="text-ui-reg">
                          {col.nullable ? "true" : "false"}
                        </Text>
                      </Box>
                    </Box>
                  ))}
                </Box>
              </Box>
            </Box>
          </Box>
        )}

        <SqlDefinitionSection object={object} />
      </VStack>
    </Box>
  );
};

/**
 * Shows the SQL definition (SHOW CREATE) for the object.
 */
const SqlDefinitionSection = ({
  object,
}: {
  object: MaintainedObjectListRow;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const objectType = object.objectType as ShowCreateObjectType;
  const isSupported = [
    "index",
    "materialized-view",
    "source",
    "sink",
    "table",
  ].includes(objectType);

  const { data: sqlDef } = useQuery({
    queryKey: [
      ...buildRegionQueryKey("maintainedObjects"),
      buildQueryKeyPart("showCreate", { id: object.id }),
    ],
    queryFn: ({ queryKey, signal }) =>
      fetchShowCreate(queryKey, {
        objectType,
        object: {
          databaseName: object.databaseName,
          schemaName: object.schemaName,
          name: object.name,
        },
      }, { signal }),
    enabled: isSupported,
    staleTime: 60_000,
    select: (data) => data.rows[0]?.sql ?? null,
  });

  if (!isSupported || !sqlDef) return null;

  return (
    <Box width="100%">
      <Text textStyle="heading-sm" mb={3}>
        SQL Definition
      </Text>
      <Box
        borderRadius="md"
        border="1px"
        borderColor={colors.border.primary}
        overflow="auto"
        maxHeight="300px"
        bg={colors.background.secondary}
        p={4}
      >
        <Code
          display="block"
          whiteSpace="pre-wrap"
          fontSize="13px"
          bg="transparent"
          color={colors.foreground.primary}
        >
          {sqlDef}
        </Code>
      </Box>
    </Box>
  );
};

/**
 * Shows source-specific diagnostics: rehydration latency, ingestion stats, snapshot progress.
 * Only rendered for source objects.
 */
const SourceDiagnostics = ({ sourceId }: { sourceId: string }) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { data: stats } = useQuery({
    queryKey: [
      ...buildRegionQueryKey("maintainedObjects"),
      buildQueryKeyPart("sourceStats", { sourceId }),
    ],
    queryFn: ({ queryKey, signal }) =>
      fetchSourceStatistics(queryKey, { sourceId }, { signal }),
    refetchInterval: 10_000,
    staleTime: 5_000,
    select: (data) => data.rows[0] ?? null,
  });

  if (!stats) return null;

  const snapshotKnown = Number(stats.snapshotRecordsKnown ?? 0);
  const snapshotStaged = Number(stats.snapshotRecordsStaged ?? 0);
  const snapshotPercent =
    snapshotKnown > 0 ? Math.round((snapshotStaged / snapshotKnown) * 100) : null;
  const snapshotComplete = snapshotPercent === 100 || snapshotPercent === null;

  const rehydrationLag = stats.rehydrationLatency as IPostgresInterval | null;
  const rehydrationMs = rehydrationLag
    ? sumPostgresIntervalMs(rehydrationLag)
    : null;

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Source diagnostics</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Ingestion health and progress for this source.
        </Text>

        <Box width="100%">
          <VStack align="start" spacing={2} width="100%">
            <StatRow
              label="Rehydration latency"
              value={
                rehydrationMs !== null
                  ? `${(rehydrationMs / 1000).toFixed(1)}s`
                  : "Still rehydrating..."
              }
              color={
                rehydrationMs === null
                  ? colors.accent.orange
                  : colors.foreground.primary
              }
            />
            <StatRow
              label="Messages received"
              value={Number(stats.messagesReceived ?? 0).toLocaleString()}
            />
            <StatRow
              label="Bytes received"
              value={formatBytesShort(BigInt(stats.bytesReceived ?? 0))}
            />
            <StatRow
              label="Updates staged"
              value={Number(stats.updatesStaged ?? 0).toLocaleString()}
            />
            <StatRow
              label="Updates committed"
              value={Number(stats.updatesCommitted ?? 0).toLocaleString()}
            />
            {!snapshotComplete && snapshotPercent !== null && (
              <>
                <StatRow
                  label="Snapshot progress"
                  value={`${snapshotPercent}% (${snapshotStaged.toLocaleString()} / ${snapshotKnown.toLocaleString()} records)`}
                  color={colors.accent.orange}
                />
                <Box width="100%" height="6px" borderRadius="full" bg={colors.background.secondary}>
                  <Box
                    height="100%"
                    width={`${snapshotPercent}%`}
                    borderRadius="full"
                    bg={colors.accent.brightPurple}
                    transition="width 0.3s ease"
                  />
                </Box>
              </>
            )}
            {snapshotComplete && (
              <StatRow
                label="Snapshot"
                value="Complete"
                color={colors.accent.green}
              />
            )}
          </VStack>
        </Box>
      </VStack>
    </Card>
  );
};

const StatRow = ({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack justify="space-between" width="100%">
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {label}
      </Text>
      <Text textStyle="text-ui-med" color={color ?? colors.foreground.primary}>
        {value}
      </Text>
    </HStack>
  );
};

/**
 * Shows per-object memory from the unified mz_internal.mz_object_arrangement_sizes
 * collection. Displays memory across all replicas — no session variables needed.
 *
 * This card exists alongside the original ObjectMemoryCard for comparison.
 * Once validated, the original can be removed and this becomes the primary.
 */
const UnifiedMemoryCard = ({
  replicas,
  isLoading,
  estimatedCreditsPerHour,
  replicaCreditsPerHour,
}: {
  replicas: Array<{
    replicaId: string;
    replicaName: string;
    memoryBytes: string;
    memoryPercentage: number | null;
  }> | null;
  isLoading: boolean;
  estimatedCreditsPerHour: number | null;
  replicaCreditsPerHour: number | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (isLoading) {
    return (
      <Card
        p={5}
        width="100%"
        borderRadius="md"
        border="1px"
        borderColor={colors.accent.brightPurple}
        bg={`${colors.accent.brightPurple}08`}
      >
        <VStack align="start" spacing={2}>
          <HStack>
            <Text textStyle="heading-sm">Memory Usage (Unified)</Text>
            <Text textStyle="text-small" color={colors.accent.brightPurple}>
              NEW
            </Text>
          </HStack>
          <HStack spacing={2}>
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Loading from mz_internal.mz_object_arrangement_sizes...
            </Text>
          </HStack>
        </VStack>
      </Card>
    );
  }

  if (!replicas || replicas.length === 0) {
    return (
      <Card
        p={5}
        width="100%"
        borderRadius="md"
        border="1px"
        borderColor={colors.accent.brightPurple}
        bg={`${colors.accent.brightPurple}08`}
      >
        <VStack align="start" spacing={2}>
          <HStack>
            <Text textStyle="heading-sm">Memory Usage (Unified)</Text>
            <Text textStyle="text-small" color={colors.accent.brightPurple}>
              NEW
            </Text>
          </HStack>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Waiting for introspection data...
          </Text>
        </VStack>
      </Card>
    );
  }

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.accent.brightPurple}
      bg={`${colors.accent.brightPurple}08`}
    >
      <VStack align="start" spacing={3} width="100%">
        <HStack>
          <Text textStyle="heading-sm">Memory Usage (Unified)</Text>
          <Text textStyle="text-small" color={colors.accent.brightPurple}>
            NEW
          </Text>
        </HStack>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          From mz_internal.mz_object_arrangement_sizes — no session variables, no cluster targeting.
        </Text>

        {replicas.map((replica) => {
          const bytes = BigInt(replica.memoryBytes);
          const percent = replica.memoryPercentage;
          const barColor =
            percent !== null && percent > 90
              ? colors.accent.red
              : percent !== null && percent > 70
                ? colors.accent.orange
                : colors.accent.green;

          return (
            <Box key={replica.replicaId} width="100%">
              <HStack width="100%" justify="space-between" mb={1}>
                <Text textStyle="text-ui-med">
                  {replica.replicaName}
                </Text>
                <HStack spacing={2}>
                  <Text textStyle="text-ui-med">
                    {formatBytesShort(bytes)}
                  </Text>
                  {percent !== null && (
                    <Text textStyle="text-small" color={barColor}>
                      ({Math.round(percent)}%)
                    </Text>
                  )}
                </HStack>
              </HStack>
              {percent !== null && (
                <Box
                  width="100%"
                  height="8px"
                  borderRadius="full"
                  bg={colors.background.secondary}
                  overflow="hidden"
                >
                  <Box
                    height="100%"
                    minWidth={bytes > 0n ? "4px" : "0px"}
                    width={`${Math.min(Math.round(percent), 100)}%`}
                    borderRadius="full"
                    bg={barColor}
                    transition="width 0.3s ease"
                  />
                </Box>
              )}
            </Box>
          );
        })}

        {estimatedCreditsPerHour !== null && (
          <Box
            width="100%"
            pt={3}
            mt={1}
            borderTop="1px"
            borderColor={colors.border.primary}
          >
            <HStack width="100%" justify="space-between">
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Estimated cost (memory-based)
              </Text>
              <Text textStyle="text-ui-med">
                {estimatedCreditsPerHour < 0.01
                  ? "<0.01"
                  : estimatedCreditsPerHour.toFixed(2)}{" "}
                credits/hr
              </Text>
            </HStack>
            {replicaCreditsPerHour !== null && (
              <Text textStyle="text-small" color={colors.foreground.secondary} mt={1}>
                Based on memory fraction of replica ({replicaCreditsPerHour.toFixed(1)} credits/hr total)
              </Text>
            )}
          </Box>
        )}
      </VStack>
    </Card>
  );
};

export default ObjectDetailPanel;
