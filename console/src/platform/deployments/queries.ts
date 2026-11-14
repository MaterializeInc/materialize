// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { millisecondsInSecond } from "date-fns/constants";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  fetchAfterSwap,
  fetchDeploymentCatalog,
  fetchInProgressStatus,
  QualifiedSchema,
} from "~/api/materialize/deployments/deploymentCatalog";
import {
  fetchHistory,
  fetchStaged,
} from "~/api/materialize/deployments/deploymentDiscovery";

import { Deployment, groupDeployments } from "./deploymentModel";

/** In-progress deploys change by the second while they hydrate. */
const LIVE_REFETCH_MS = 5 * millisecondsInSecond;
const LIST_REFETCH_MS = 15 * millisecondsInSecond;
/**
 * History scans all of `mz_audit_events`, which grows for the life of the
 * environment, and only changes when a deploy finishes.
 */
const HISTORY_REFETCH_MS = 60 * millisecondsInSecond;

interface CatalogInput {
  clusterNames: string[];
  liveSchemas: QualifiedSchema[];
  stagedSchemas: QualifiedSchema[];
  suffix: string;
}

interface InProgressInput {
  clusterNames: string[];
  deploys: { deployId: string; schemas: QualifiedSchema[] }[];
}

const schemaList = (schemas: QualifiedSchema[]) =>
  schemas.map((s) => `${s.database}.${s.schema}`).join(",");

export const deploymentQueryKeys = {
  all: () => buildRegionQueryKey("deployments"),
  staged: () =>
    [...deploymentQueryKeys.all(), buildQueryKeyPart("staged")] as const,
  history: () =>
    [...deploymentQueryKeys.all(), buildQueryKeyPart("history")] as const,
  inProgress: (input: InProgressInput) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("inProgress", {
        clusterNames: input.clusterNames.join(","),
        deploys: input.deploys
          .map((d) => `${d.deployId}:${schemaList(d.schemas)}`)
          .join(";"),
      }),
    ] as const,
  catalog: (key: string, input: CatalogInput | null) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("catalog", {
        key,
        clusterNames: input?.clusterNames.join(",") ?? "",
        liveSchemas: schemaList(input?.liveSchemas ?? []),
        stagedSchemas: schemaList(input?.stagedSchemas ?? []),
        suffix: input?.suffix ?? "",
      }),
    ] as const,
  afterSwap: (key: string, clusterIds: string[], promotedAt: Date | null) =>
    [
      ...deploymentQueryKeys.all(),
      buildQueryKeyPart("afterSwap", {
        key,
        clusterIds: clusterIds.join(","),
        promotedAt: promotedAt?.toISOString() ?? "",
      }),
    ] as const,
};

const liveSchemas = (d: Deployment) =>
  d.schemas.map((s) => ({ database: s.database ?? "", schema: s.liveName }));

/** Every deploy the catalog shows: staged now, promoted or aborted. */
export function useDeployments() {
  const staged = useQuery({
    queryKey: deploymentQueryKeys.staged(),
    queryFn: ({ queryKey, signal }) =>
      fetchStaged({ queryKey, requestOptions: { signal } }),
    refetchInterval: LIST_REFETCH_MS,
  });
  const history = useQuery({
    queryKey: deploymentQueryKeys.history(),
    queryFn: ({ queryKey, signal }) =>
      fetchHistory({ queryKey, requestOptions: { signal } }),
    refetchInterval: HISTORY_REFETCH_MS,
  });
  const data = React.useMemo(
    () =>
      staged.data && history.data
        ? groupDeployments(
            staged.data.staged,
            history.data,
            staged.data.hasMzDeploy,
          )
        : undefined,
    [staged.data, history.data],
  );
  return {
    data,
    isLoading: staged.isLoading || history.isLoading,
    error: staged.error ?? history.error,
  };
}

/** Staged cluster health and sink cutovers of every in-progress deploy. */
export function useInProgressStatus(deployments: Deployment[] | undefined) {
  const input = React.useMemo((): InProgressInput => {
    const inProgress = (deployments ?? []).filter(
      (d) => d.state === "in-progress",
    );
    return {
      clusterNames: inProgress.flatMap((d) =>
        d.clusters.map((c) => c.stagedName),
      ),
      deploys: inProgress.map((d) => ({
        deployId: d.suffix,
        schemas: liveSchemas(d),
      })),
    };
  }, [deployments]);
  return useQuery({
    queryKey: deploymentQueryKeys.inProgress(input),
    queryFn: ({ queryKey, signal }) =>
      fetchInProgressStatus({
        ...input,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: input.deploys.length > 0,
    refetchInterval: LIVE_REFETCH_MS,
    placeholderData: keepPreviousData,
  });
}

/** One deploy, and for an in-progress one, the live state of what it staged. */
export function useDeploymentDetail(key: string) {
  const list = useDeployments();
  const deployment = list.data?.find((d) => d.key === key);

  const input = React.useMemo((): CatalogInput | null => {
    if (!deployment) return null;
    return {
      clusterNames: deployment.clusters.flatMap((c) => [
        c.liveName,
        c.stagedName,
      ]),
      liveSchemas: liveSchemas(deployment),
      stagedSchemas: deployment.schemas.map((s) => ({
        database: s.database ?? "",
        schema: s.stagedName,
      })),
      suffix: deployment.suffix,
    };
  }, [deployment]);

  const catalog = useQuery({
    queryKey: deploymentQueryKeys.catalog(key, input),
    queryFn: ({ queryKey, signal }) => {
      if (!input) return null;
      return fetchDeploymentCatalog({
        ...input,
        queryKey,
        requestOptions: { signal },
      });
    },
    enabled: deployment?.state === "in-progress",
    refetchInterval: LIVE_REFETCH_MS,
    placeholderData: keepPreviousData,
  });

  return { list, deployment, catalog };
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
  const { key, clusters, endedAt, state } = deployment;
  return useQuery({
    queryKey: deploymentQueryKeys.afterSwap(
      key,
      clusters.map((c) => c.id),
      endedAt,
    ),
    queryFn: ({ queryKey, signal }) => {
      if (!endedAt) return [];
      return fetchAfterSwap({
        clusters,
        promotedAt: endedAt,
        queryKey,
        requestOptions: { signal },
      });
    },
    enabled: state === "promoted" && endedAt !== null,
  });
}
