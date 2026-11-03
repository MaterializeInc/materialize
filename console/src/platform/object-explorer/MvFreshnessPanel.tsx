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
  Code,
  HStack,
  IconButton,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useSuspenseQuery } from "@tanstack/react-query";
import copyToClipboard from "copy-to-clipboard";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { mcpQuerySystemCatalog } from "~/api/materialize/mcpClient";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { useToast } from "~/hooks/useToast";
import CopyIcon from "~/svg/CopyIcon";
import { MaterializeTheme } from "~/theme";

interface MvFreshnessData {
  localLag: string;
  globalLag: string;
  hydrated: boolean | null;
  bottleneckName: string | null;
  bottleneckType: string | null;
  bottleneckCluster: string | null;
  mvCluster: string | null;
}

function buildFreshnessQuery(objectId: string) {
  return `SELECT
  lag.local_lag::text AS local_lag,
  lag.global_lag::text AS global_lag,
  hs.hydrated,
  slowest_local.name AS bottleneck_name,
  slowest_local.type AS bottleneck_type,
  bc.name AS bottleneck_cluster,
  mc.name AS mv_cluster
FROM mz_internal.mz_materialization_lag lag
LEFT JOIN mz_internal.mz_hydration_statuses hs ON hs.object_id = lag.object_id
LEFT JOIN mz_objects slowest_local ON slowest_local.id = lag.slowest_local_input_id
LEFT JOIN mz_clusters bc ON bc.id = slowest_local.cluster_id
LEFT JOIN mz_objects mv ON mv.id = lag.object_id
LEFT JOIN mz_clusters mc ON mc.id = mv.cluster_id
WHERE lag.object_id = '${objectId}'
LIMIT 1`;
}

function parseFreshnessData(raw: string): MvFreshnessData | null {
  try {
    const rows: unknown[][] = JSON.parse(raw);
    if (rows.length === 0) return null;
    const row = rows[0];
    return {
      localLag: String(row[0] ?? "unknown"),
      globalLag: String(row[1] ?? "unknown"),
      hydrated: row[2] === null ? null : row[2] === true || row[2] === "t",
      bottleneckName: row[3] ? String(row[3]) : null,
      bottleneckType: row[4] ? String(row[4]) : null,
      bottleneckCluster: row[5] ? String(row[5]) : null,
      mvCluster: row[6] ? String(row[6]) : null,
    };
  } catch {
    return null;
  }
}

function lagSeverity(lag: string): "fresh" | "warning" | "critical" {
  if (lag === "00:00:00" || lag.startsWith("00:00:0")) return "fresh";
  // Anything over 1 minute is critical
  if (!lag.startsWith("00:00:")) return "critical";
  return "warning";
}

const mvFreshnessQueryKeys = {
  byId: (objectId: string) =>
    [
      ...buildRegionQueryKey("mvFreshness"),
      buildQueryKeyPart("mvFreshness", { objectId }),
    ] as const,
};

const MvFreshnessPanelContent = ({ objectId }: { objectId: string }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const toast = useToast();

  const { data } = useSuspenseQuery({
    queryKey: mvFreshnessQueryKeys.byId(objectId),
    queryFn: async () => {
      const raw = await mcpQuerySystemCatalog(buildFreshnessQuery(objectId));
      return parseFreshnessData(raw);
    },
    refetchInterval: 10_000,
  });

  if (!data) return null;

  const severity = lagSeverity(data.localLag);

  const severityColors = {
    fresh: colors.accent.green,
    warning: colors.accent.darkYellow,
    critical: colors.accent.red,
  };

  const severityLabels = {
    fresh: "Fresh",
    warning: "Lagging",
    critical: "Lagging",
  };

  const suggestedFix =
    data.bottleneckCluster && data.mvCluster !== data.bottleneckCluster
      ? `-- Consider moving this MV closer to its bottleneck input\nALTER MATERIALIZED VIEW ... SET CLUSTER = '${data.bottleneckCluster}';`
      : null;

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      borderRadius="lg"
      borderWidth="1px"
      borderColor={
        severity === "critical"
          ? colors.accent.red
          : severity === "warning"
            ? colors.accent.darkYellow
            : colors.border.secondary
      }
      spacing="0"
    >
      <VStack
        alignItems="flex-start"
        width="100%"
        padding="4"
        spacing="3"
        bg={
          severity === "critical"
            ? "red.50"
            : severity === "warning"
              ? "yellow.50"
              : undefined
        }
        borderTopRadius="lg"
      >
        <HStack spacing="3" width="100%">
          <Box
            width="3"
            height="3"
            borderRadius="full"
            bg={severityColors[severity]}
            flexShrink={0}
          />
          <Text textStyle="heading-sm" color={colors.foreground.primary}>
            Freshness: {severityLabels[severity]}
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            via MCP
          </Text>
        </HStack>

        <HStack spacing="6" flexWrap="wrap">
          <VStack alignItems="flex-start" spacing="0">
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Local lag
            </Text>
            <Text textStyle="text-ui-med">{data.localLag}</Text>
          </VStack>
          <VStack alignItems="flex-start" spacing="0">
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Global lag
            </Text>
            <Text textStyle="text-ui-med">{data.globalLag}</Text>
          </VStack>
          <VStack alignItems="flex-start" spacing="0">
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Hydrated
            </Text>
            <Text textStyle="text-ui-med">
              {data.hydrated === null
                ? "Unknown"
                : data.hydrated
                  ? "Yes"
                  : "No"}
            </Text>
          </VStack>
        </HStack>

        {data.bottleneckName && (
          <VStack alignItems="flex-start" spacing="0">
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Slowest input (bottleneck)
            </Text>
            <Text textStyle="text-ui-med">
              {data.bottleneckName}
              {data.bottleneckType && ` (${data.bottleneckType})`}
              {data.bottleneckCluster &&
                ` on cluster ${data.bottleneckCluster}`}
            </Text>
          </VStack>
        )}

        {suggestedFix && severity !== "fresh" && (
          <VStack alignItems="flex-start" spacing="1" width="100%">
            <HStack spacing="2">
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                Suggested fix
              </Text>
              <Tooltip label="Copy SQL" fontSize="xs">
                <IconButton
                  icon={<CopyIcon />}
                  aria-label="Copy suggested fix"
                  onClick={() => {
                    copyToClipboard(suggestedFix);
                    toast({ description: "SQL copied to clipboard" });
                  }}
                  variant="inline"
                  size="xs"
                />
              </Tooltip>
            </HStack>
            <Code
              display="block"
              whiteSpace="pre"
              px="3"
              py="2"
              borderRadius="md"
              fontSize="sm"
              width="100%"
            >
              {suggestedFix}
            </Code>
          </VStack>
        )}
      </VStack>
    </VStack>
  );
};

export interface MvFreshnessPanelProps {
  objectId: string;
}

const MvFreshnessPanel = ({ objectId }: MvFreshnessPanelProps) => {
  return (
    <AppErrorBoundary
      message="Unable to fetch freshness data from the MCP developer endpoint."
      containerProps={{ padding: "4" }}
    >
      <React.Suspense
        fallback={
          <Box height="80px" width="100%">
            <LoadingContainer />
          </Box>
        }
      >
        <MvFreshnessPanelContent objectId={objectId} />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export default MvFreshnessPanel;
