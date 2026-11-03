// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme } from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { mcpQuerySystemCatalog } from "~/api/materialize/mcpClient";
import Alert from "~/components/Alert";
import { MaterializeTheme } from "~/theme";

interface ClusterImpact {
  affectedCount: number;
  affectedNames: string;
}

function buildImpactQuery(clusterId: string) {
  return `SELECT
  count(DISTINCT dep.object_id)::text AS affected_count,
  string_agg(DISTINCT o.name, ', ' ORDER BY o.name) AS affected_names
FROM mz_internal.mz_object_dependencies dep
JOIN mz_objects src ON src.id = dep.referenced_object_id
JOIN mz_objects o ON o.id = dep.object_id AND o.cluster_id != src.cluster_id
JOIN mz_internal.mz_materialization_lag lag ON lag.object_id = dep.object_id
WHERE src.cluster_id = '${clusterId}'
  AND lag.local_lag >= INTERVAL '10 seconds'`;
}

function parseImpactData(raw: string): ClusterImpact | null {
  try {
    const rows: unknown[][] = JSON.parse(raw);
    if (rows.length === 0) return null;
    const row = rows[0];
    const count = parseInt(String(row[0] ?? "0"), 10);
    if (count === 0) return null;
    return {
      affectedCount: count,
      affectedNames: String(row[1] ?? ""),
    };
  } catch {
    return null;
  }
}

const clusterImpactQueryKeys = {
  byId: (clusterId: string) =>
    [
      ...buildRegionQueryKey("clusterImpact"),
      buildQueryKeyPart("clusterImpact", { clusterId }),
    ] as const,
};

export interface ClusterImpactBannerProps {
  clusterId: string;
}

const ClusterImpactBanner = ({ clusterId }: ClusterImpactBannerProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const { data: impact } = useQuery({
    queryKey: clusterImpactQueryKeys.byId(clusterId),
    queryFn: async () => {
      const raw = await mcpQuerySystemCatalog(buildImpactQuery(clusterId));
      return parseImpactData(raw);
    },
    refetchInterval: 30_000,
  });

  if (!impact) return null;

  return (
    <Alert
      variant="warning"
      width="100%"
      message={
        <Text>
          This cluster has{" "}
          <Text as="span" fontWeight="600">
            {impact.affectedCount} materialized view
            {impact.affectedCount !== 1 ? "s" : ""}
          </Text>{" "}
          on other clusters that are lagging as a result:{" "}
          <Text as="span" color={colors.foreground.secondary}>
            {impact.affectedNames}
          </Text>
        </Text>
      }
    />
  );
};

export default ClusterImpactBanner;
