// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import { useCallback } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  BlockedDependencyInfo,
  fetchBlockedDependencies,
} from "~/api/materialize/cluster/materializationLag";
import {
  fetchNonRunningSources,
  NonRunningSourceInfo,
} from "~/api/materialize/source/nonRunningSources";

import { HistoryId } from "../historyId";

// We need to reference each specific query to avoid queries with the parameters reusing incorrect results.
type CommandResultInfo = {
  historyId: HistoryId;
  commandResultIndex: number;
  hasSuccessfullyFetchedOnce: boolean;
};

type BlockedDependenciesQueryKeyParams = {
  objectIds: string[];
} & CommandResultInfo;

type NonRunningSourcesQueryKeyParams = {
  objectIds: string[];
} & CommandResultInfo;

export const planInsightsQueryKeys = {
  all: () => buildRegionQueryKey("planInsights"),
  blockedDependencies: (params: BlockedDependenciesQueryKeyParams) =>
    [
      ...planInsightsQueryKeys.all(),
      buildQueryKeyPart("blockedDependencies", params),
    ] as const,
  nonRunningSources: (params: NonRunningSourcesQueryKeyParams) =>
    [
      ...planInsightsQueryKeys.all(),
      buildQueryKeyPart("nonRunningSources", params),
    ] as const,
};

/**
 * Keyed by an object, information about the dependencies that are blocking said object
 */
export type BlockedDependencies = {
  [objectId: string]: BlockedDependencyInfo;
};

/**
 * Keyed by a source, information about if the source does not have the status 'running'
 */
export type NonRunningSources = {
  [sourceId: string]: NonRunningSourceInfo;
};

// We don't want these queries to refetch given they represent a command ran at a fixed point in time
const FETCH_ONCE_QUERY_OPTIONS = {
  retry: false,
  staleTime: Number.POSITIVE_INFINITY,
  gcTime: Number.POSITIVE_INFINITY,
};

/**
 * Fetches a normalized table of an object, the lag between its direct parent, and
 * the lag between its source/table objects
 */
export function useBlockedDependencies(
  params: BlockedDependenciesQueryKeyParams,
) {
  const blockedDependenciesSelector = useCallback(
    (lagData: Awaited<ReturnType<typeof fetchBlockedDependencies>>) => {
      const blockedDependencies: BlockedDependencies = {};
      for (const r of lagData?.rows ?? []) {
        blockedDependencies[r.targetObjectId] = r;
      }

      return {
        blockedDependencies,
      };
    },
    [],
  );

  return useQuery({
    queryKey: planInsightsQueryKeys.blockedDependencies(params),
    queryFn: ({ queryKey, signal }) => {
      return fetchBlockedDependencies(params, queryKey, { signal });
    },

    select: blockedDependenciesSelector,
    enabled: params.objectIds?.length > 0 && !params.hasSuccessfullyFetchedOnce,
    ...FETCH_ONCE_QUERY_OPTIONS,
  });
}

/**
 * Fetches a normalized table of an object, the lag between its direct parent, and
 * the lag between its source/table objects
 */
export function useNonRunningSources(params: NonRunningSourcesQueryKeyParams) {
  const nonRunningSourcesSelector = useCallback(
    (
      nonRunningSourcesData: Awaited<ReturnType<typeof fetchNonRunningSources>>,
    ) => {
      const nonRunningSources: NonRunningSources = {};
      for (const r of nonRunningSourcesData?.rows ?? []) {
        nonRunningSources[r.id] = r;
      }

      return {
        nonRunningSources,
      };
    },
    [],
  );

  return useQuery({
    queryKey: planInsightsQueryKeys.nonRunningSources(params),
    queryFn: ({ queryKey, signal }) => {
      return fetchNonRunningSources(params, queryKey, { signal });
    },

    select: nonRunningSourcesSelector,
    enabled: params.objectIds?.length > 0 && !params.hasSuccessfullyFetchedOnce,
    ...FETCH_ONCE_QUERY_OPTIONS,
  });
}
