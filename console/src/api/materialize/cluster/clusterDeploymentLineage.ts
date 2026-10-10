// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { executeSqlV2, queryBuilder } from "..";

type ClusterDeploymentLineageParams = {
  clusterIds?: string[];
};

export const buildClusterDeploymentLineageQuery = (
  params: ClusterDeploymentLineageParams,
) => {
  let query = queryBuilder
    .selectFrom("mz_cluster_deployment_lineage")
    .select([
      "cluster_id as clusterId",
      "current_deployment_cluster_id as currentDeploymentClusterId",
      "cluster_name as clusterName",
    ]);

  if (params.clusterIds && params.clusterIds.length > 0) {
    query = query.where(
      "current_deployment_cluster_id",
      "in",
      params.clusterIds,
    );
  }

  return query;
};

type DeploymentRow = {
  clusterId: string;
  currentDeploymentClusterId: string;
  clusterName: string;
};

/**
 * Fetches blue-green lineage information for clusters in the current environment.
 */
export async function fetchClusterDeploymentLineage({
  params,
  queryKey,
  requestOptions,
}: {
  params: ClusterDeploymentLineageParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildClusterDeploymentLineageQuery({
    clusterIds: params.clusterIds,
  }).compile();

  const pastDeploymentsByCurrentDeployment = new Map<string, DeploymentRow[]>();
  const currentDeploymentByPastDeployment = new Map<string, DeploymentRow>();

  const res = await executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
    // Callers refetch lineage periodically, so a stale read only delays a
    // blue-green cutover. Strict serializable would block the read until the
    // lineage index catches up, which is slowest when mz_catalog_server is busy.
    sessionVariables: { transaction_isolation: "serializable" },
  });

  for (const row of res.rows) {
    if (row.clusterId === null) {
      continue;
    }

    // Find the current deployment cluster ID for each cluster
    if (!currentDeploymentByPastDeployment.has(row.clusterId)) {
      currentDeploymentByPastDeployment.set(row.clusterId, {
        clusterId: row.clusterId,
        currentDeploymentClusterId: row.currentDeploymentClusterId,
        clusterName: row.clusterName,
      });
    }

    // Find all past deployments for each current deployment
    if (
      !pastDeploymentsByCurrentDeployment.has(row.currentDeploymentClusterId)
    ) {
      pastDeploymentsByCurrentDeployment.set(
        row.currentDeploymentClusterId,
        [],
      );
    }
    const pastDeployments = pastDeploymentsByCurrentDeployment.get(
      row.currentDeploymentClusterId,
    );
    pastDeployments?.push({
      clusterId: row.clusterId,
      currentDeploymentClusterId: row.currentDeploymentClusterId,
      clusterName: row.clusterName,
    });
  }

  return {
    pastDeploymentsByCurrentDeployment,
    currentDeploymentByPastDeployment,
  };
}

/**
 * Expands each of `clusterIds` to every cluster in its blue-green lineage. The
 * result is sorted and deduplicated, so equal lineages yield equal arrays
 * regardless of row order.
 */
export function expandClusterLineage(
  clusterIds: string[],
  pastDeploymentsByCurrentDeployment: Map<string, DeploymentRow[]>,
): string[] {
  const expanded = new Set<string>();
  for (const clusterId of clusterIds) {
    const lineage = pastDeploymentsByCurrentDeployment.get(clusterId);
    if (lineage) {
      for (const deployment of lineage) {
        expanded.add(deployment.clusterId);
      }
    } else {
      // System clusters have no lineage rows, so keep the id itself.
      expanded.add(clusterId);
    }
  }
  return [...expanded].sort();
}
