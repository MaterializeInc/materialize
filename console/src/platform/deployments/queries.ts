// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { keepPreviousData, useQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { PermissionError } from "~/api/materialize/DatabaseError";
import {
  fetchAfterSwap,
  fetchClusterHealth,
  fetchDeploymentCatalog,
} from "~/api/materialize/deployments/deploymentCatalog";
import {
  fetchDeploymentDetail,
  fetchDeployments,
  isMzDeployMissing,
} from "~/api/materialize/deployments/mzDeploy";

import {
  baselineDeploys,
  Deployment,
  groupDeployments,
  stagingName,
  swappedSchemas,
} from "./deploymentModel";

/** In-progress deploys change by the second while they hydrate. */
const LIVE_REFETCH_MS = 5_000;
const LIST_REFETCH_MS = 15_000;

export const deploymentQueryKeys = {
  all: () => buildRegionQueryKey("deployments"),
  list: () =>
    [...deploymentQueryKeys.all(), buildQueryKeyPart("list")] as const,
  listHealth: (clusterNames: string[]) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("listHealth", {
        clusterNames: [...clusterNames].sort().join(","),
      }),
    ] as const,
  detail: (deployIds: string[]) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("detail", { deployIds: deployIds.join(",") }),
    ] as const,
  afterSwap: (deployId: string, promotedAt: Date | null) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("afterSwap", {
        deployId,
        promotedAt: promotedAt?.toISOString() ?? null,
      }),
    ] as const,
  catalog: (
    deployId: string,
    input: {
      clusterNames: string[];
      schemas: { database: string; schema: string }[];
      pendingSinks: { database: string; schema: string; name: string }[];
    },
  ) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("catalog", {
        deployId,
        clusterNames: [...input.clusterNames].sort().join(","),
        schemas: input.schemas
          .map((s) => `${s.database}.${s.schema}`)
          .sort()
          .join(","),
        pendingSinks: input.pendingSinks
          .map((k) => `${k.database}.${k.schema}.${k.name}`)
          .sort()
          .join(","),
      }),
    ] as const,
};

export interface DeploymentsData {
  /**
   * `missing` when `_mz_deploy` does not exist (Materialize also reports it
   * missing to a user without USAGE on it), `denied` when the user can see it
   * but not read it.
   */
  access: "ok" | "missing" | "denied";
  deployments: Deployment[];
  /** Production cluster names each deploy stages, keyed by deploy ID. */
  clustersByDeploy: Map<string, string[]>;
}

export function useDeployments() {
  return useQuery({
    queryKey: deploymentQueryKeys.list(),
    queryFn: async ({ queryKey, signal }): Promise<DeploymentsData> => {
      try {
        const { deployments, clusters } = await fetchDeployments({
          queryKey,
          requestOptions: { signal },
        });
        const clustersByDeploy = new Map<string, string[]>();
        for (const { deployId, name } of clusters) {
          clustersByDeploy.set(deployId, [
            ...(clustersByDeploy.get(deployId) ?? []),
            name,
          ]);
        }
        return {
          access: "ok",
          deployments: groupDeployments(deployments),
          clustersByDeploy,
        };
      } catch (error) {
        if (isMzDeployMissing(error) || error instanceof PermissionError) {
          return {
            access: error instanceof PermissionError ? "denied" : "missing",
            deployments: [],
            clustersByDeploy: new Map(),
          };
        }
        throw error;
      }
    },
    refetchInterval: LIST_REFETCH_MS,
  });
}

/** Health of every in-progress deploy's staged clusters, for the list page. */
export function useStagedClusterHealth(data: DeploymentsData | undefined) {
  const clusterNames = React.useMemo(() => {
    if (!data) return [];
    return data.deployments
      .filter((d) => d.promotedAt === null)
      .flatMap((d) =>
        (data.clustersByDeploy.get(d.deployId) ?? []).map((name) =>
          stagingName(name, d.deployId),
        ),
      );
  }, [data]);
  return useQuery({
    queryKey: deploymentQueryKeys.listHealth(clusterNames),
    queryFn: ({ queryKey, signal }) =>
      fetchClusterHealth({
        clusterNames,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: clusterNames.length > 0,
    refetchInterval: LIVE_REFETCH_MS,
    placeholderData: keepPreviousData,
  });
}

/**
 * Everything the detail page shows for one deploy: tracking rows from
 * `_mz_deploy`, then the live catalog state of the clusters, schemas and sinks
 * those rows name.
 */
export function useDeploymentDetail(deployId: string) {
  const list = useDeployments();
  const deployment = list.data?.deployments.find(
    (d) => d.deployId === deployId,
  );
  const isInProgress = deployment?.promotedAt === null;

  const baseline = React.useMemo(
    () =>
      deployment && list.data
        ? baselineDeploys(deployment, list.data.deployments)
        : new Map<string, string>(),
    [deployment, list.data],
  );
  const deployIds = React.useMemo(
    () => [deployId, ...new Set(baseline.values())],
    [deployId, baseline],
  );

  const tracking = useQuery({
    queryKey: deploymentQueryKeys.detail(deployIds),
    queryFn: ({ queryKey, signal }) =>
      fetchDeploymentDetail({
        deployIds,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: deployment !== undefined,
    refetchInterval: isInProgress ? LIVE_REFETCH_MS : false,
  });

  const liveClusters = React.useMemo(
    () => list.data?.clustersByDeploy.get(deployId) ?? [],
    [list.data, deployId],
  );
  const catalogInput = React.useMemo(() => {
    const live = deployment ? swappedSchemas(deployment) : [];
    const staged = live.map((s) => ({
      database: s.database,
      schema: stagingName(s.schema, deployId),
    }));
    const pendingSinks = (tracking.data?.pendingStatements ?? [])
      .filter((p) => p.statementKind === "sink")
      .map((p) => ({ database: p.database, schema: p.schema, name: p.object }));
    return {
      clusterNames: [
        ...liveClusters,
        ...liveClusters.map((name) => stagingName(name, deployId)),
      ],
      liveSchemas: live.map(({ database, schema }) => ({ database, schema })),
      schemas: staged,
      pendingSinks,
    };
  }, [deployment, deployId, liveClusters, tracking.data]);

  const catalog = useQuery({
    queryKey: deploymentQueryKeys.catalog(deployId, catalogInput),
    queryFn: ({ queryKey, signal }) =>
      fetchDeploymentCatalog({
        ...catalogInput,
        deployId,
        queryKey,
        requestOptions: { signal },
      }),
    // Waits for the pending CREATE SINK statements, which change the query key.
    enabled: isInProgress && tracking.isSuccess,
    refetchInterval: LIVE_REFETCH_MS,
    placeholderData: keepPreviousData,
  });

  return { list, deployment, baseline, tracking, catalog, liveClusters };
}

/**
 * How long each sink's write frontier has gone unchanged, measured across
 * polls. A single reading cannot tell a slow sink from a stopped one.
 */
export function useFrontierStalls(
  frontiers: { id: string; frontier: number | null }[] | undefined,
) {
  const firstSeen = React.useRef(
    new Map<string, { frontier: number | null; since: number }>(),
  );
  return React.useMemo(() => {
    const now = Date.now();
    const stalls = new Map<string, number>();
    for (const { id, frontier } of frontiers ?? []) {
      const seen = firstSeen.current.get(id);
      if (!seen || seen.frontier !== frontier) {
        firstSeen.current.set(id, { frontier, since: now });
        stalls.set(id, 0);
      } else {
        stalls.set(id, now - seen.since);
      }
    }
    return stalls;
  }, [frontiers]);
}

/** Load on a promoted deploy's clusters in the window right after promote. */
export function useAfterSwap(deployment: Deployment) {
  const { deployId, promotedAt } = deployment;
  return useQuery({
    queryKey: deploymentQueryKeys.afterSwap(deployId, promotedAt),
    queryFn: ({ queryKey, signal }) => {
      if (!promotedAt) return [];
      return fetchAfterSwap({
        deployId,
        promotedAt,
        queryKey,
        requestOptions: { signal },
      });
    },
    enabled: promotedAt !== null,
  });
}
