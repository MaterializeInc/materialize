// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Badge,
  Box,
  Circle,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useSuspenseQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { mcpQuerySystemCatalog } from "~/api/materialize/mcpClient";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MaterializeTheme } from "~/theme";

interface AttentionItem {
  category: string;
  objectName: string;
  severity: "critical" | "warning" | "info";
  detail: string;
}

const ATTENTION_QUERY = `SELECT * FROM (
  SELECT
    'Cluster' AS category,
    c.name || '/' || cr.name AS object_name,
    CASE
      WHEN u.memory_percent > 90 THEN 'critical'
      WHEN u.memory_percent > 70 THEN 'warning'
      ELSE 'info'
    END AS severity,
    'Memory ' || round(u.memory_percent::numeric, 1) || '%, CPU ' || round(u.cpu_percent::numeric, 1) || '%' AS detail
  FROM mz_cluster_replicas cr
  JOIN mz_clusters c ON c.id = cr.cluster_id
  JOIN mz_internal.mz_cluster_replica_utilization u ON u.replica_id = cr.id
  WHERE c.id LIKE 'u%' AND u.memory_percent > 70

  UNION ALL

  SELECT
    'MV Lag' AS category,
    o.name AS object_name,
    CASE
      WHEN lag.local_lag >= INTERVAL '1 minute' THEN 'critical'
      ELSE 'warning'
    END AS severity,
    'Lag: ' || lag.local_lag::text AS detail
  FROM mz_internal.mz_materialization_lag lag
  JOIN mz_objects o ON o.id = lag.object_id
  WHERE o.id LIKE 'u%' AND lag.local_lag >= INTERVAL '10 seconds'

  UNION ALL

  SELECT
    'Hydration' AS category,
    o.name AS object_name,
    'warning' AS severity,
    'Not yet hydrated on replica ' || cr.name AS detail
  FROM mz_internal.mz_hydration_statuses hs
  JOIN mz_objects o ON o.id = hs.object_id
  JOIN mz_cluster_replicas cr ON cr.id = hs.replica_id
  WHERE hs.hydrated = false AND o.id LIKE 'u%'

  UNION ALL

  SELECT
    'Source' AS category,
    s.name AS object_name,
    CASE s.status WHEN 'stalled' THEN 'critical' WHEN 'failed' THEN 'critical' ELSE 'warning' END AS severity,
    COALESCE(s.error, 'Status: ' || s.status) AS detail
  FROM mz_internal.mz_source_statuses s
  WHERE s.id LIKE 'u%' AND s.status NOT IN ('running', 'created')
) attention
ORDER BY
  CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END`;

function parseAttentionItems(raw: string): AttentionItem[] {
  try {
    const rows: unknown[][] = JSON.parse(raw);
    return rows.map((row) => ({
      category: String(row[0] ?? ""),
      objectName: String(row[1] ?? ""),
      severity: String(row[2] ?? "info") as AttentionItem["severity"],
      detail: String(row[3] ?? ""),
    }));
  } catch {
    return [];
  }
}

const attentionQueryKeys = {
  all: () =>
    [
      ...buildRegionQueryKey("attentionFeed"),
      buildQueryKeyPart("attentionFeed"),
    ] as const,
};

function useAttentionFeed() {
  return useSuspenseQuery({
    queryKey: attentionQueryKeys.all(),
    queryFn: async () => {
      const raw = await mcpQuerySystemCatalog(ATTENTION_QUERY);
      return parseAttentionItems(raw);
    },
    refetchInterval: 30_000,
  });
}

const severityColor = (
  severity: AttentionItem["severity"],
  colors: MaterializeTheme["colors"],
) => {
  switch (severity) {
    case "critical":
      return colors.accent.red;
    case "warning":
      return colors.accent.darkYellow;
    case "info":
      return colors.accent.green;
  }
};

const severityBadgeScheme = (severity: AttentionItem["severity"]) => {
  switch (severity) {
    case "critical":
      return "red";
    case "warning":
      return "yellow";
    case "info":
      return "green";
  }
};

const AttentionFeedContent = () => {
  const { data: items } = useAttentionFeed();
  const { colors } = useTheme<MaterializeTheme>();

  if (items.length === 0) {
    return (
      <HStack padding="4" spacing="3">
        <Circle size="3" bg={colors.accent.green} />
        <Text textStyle="text-ui-med" color={colors.foreground.primary}>
          All systems healthy
        </Text>
      </HStack>
    );
  }

  return (
    <VStack
      alignItems="stretch"
      spacing="0"
      divider={
        <Box borderBottomWidth="1px" borderColor={colors.border.secondary} />
      }
    >
      {items.map((item, idx) => (
        <HStack key={idx} padding="4" spacing="3" alignItems="flex-start">
          <Circle
            size="3"
            bg={severityColor(item.severity, colors)}
            mt="1"
            flexShrink={0}
          />
          <VStack alignItems="flex-start" spacing="0" minWidth="0">
            <HStack spacing="2">
              <Badge
                colorScheme={severityBadgeScheme(item.severity)}
                fontSize="xs"
              >
                {item.category}
              </Badge>
              <Text
                textStyle="text-ui-med"
                color={colors.foreground.primary}
                noOfLines={1}
              >
                {item.objectName}
              </Text>
            </HStack>
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              noOfLines={2}
            >
              {item.detail}
            </Text>
          </VStack>
        </HStack>
      ))}
    </VStack>
  );
};

const AttentionFeed = () => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      alignItems="flex-start"
      width="100%"
      gap="0"
      borderRadius="lg"
      borderWidth="1px"
      spacing="0"
    >
      <VStack
        alignItems="flex-start"
        gap="1"
        width="100%"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        padding="4"
      >
        <Text textStyle="heading-md" color={colors.foreground.primary}>
          Needs attention
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Issues detected across your environment via the MCP developer
          endpoint. Auto-refreshes every 30 seconds.
        </Text>
      </VStack>
      <AppErrorBoundary
        message="Unable to fetch diagnostics from the MCP developer endpoint. The endpoint may be disabled."
        containerProps={{ padding: "4" }}
      >
        <React.Suspense
          fallback={
            <Box height="120px" width="100%">
              <LoadingContainer />
            </Box>
          }
        >
          <AttentionFeedContent />
        </React.Suspense>
      </AppErrorBoundary>
    </VStack>
  );
};

export default AttentionFeed;
