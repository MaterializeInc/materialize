// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryClient, QueryKey } from "@tanstack/react-query";

/**
 * Helper function that returns the most recent successful query data for a given query key.
 */
export function getRecentQueryData<TQueryFnData>({
  queryClient,
  queryKey,
}: {
  queryClient: QueryClient;
  queryKey?: QueryKey;
}) {
  const queriesData = queryClient.getQueriesData<TQueryFnData>({ queryKey });

  // Prune the list of queries to only include successful ones
  const filteredQueryData = queriesData.filter(
    (queryData): queryData is [QueryKey, TQueryFnData] => {
      const [key, data] = queryData;
      return (
        queryClient.getQueryState(key)?.status === "success" &&
        data !== undefined
      );
    },
  );

  if (filteredQueryData.length === 0) {
    return undefined;
  }

  filteredQueryData.sort(([queryKeyA], [queryKeyB]) => {
    const queryStateA = queryClient.getQueryState(queryKeyA);
    const queryStateB = queryClient.getQueryState(queryKeyB);

    return (
      (queryStateB?.dataUpdatedAt ?? 0) - (queryStateA?.dataUpdatedAt ?? 0)
    );
  });

  return filteredQueryData;
}

/**
 *
 * Builders used to define the return types of queryFnData and placeholderData when getting partial
 * data from another query but the shape of it doesn't fully match the current query's data. Expects
 * TPlaceholderData to be the type of the partial data.
 *
 * Instead of molding the partial data to fit the current query's data, we store it in a new property
 * called `initialPlaceholderData` and can use the runtime variable `isPlaceholderData`, given by `useQuery`,
 * to differentiate. The caveat is we force each property in the current query's data to be typed as Partial.
 *
 */
export function initialPlaceholderDataBuilders<TPlaceholderData = never>() {
  return {
    buildQueryFnReturn: <TQueryFnData>(queryFnData: TQueryFnData) => {
      return {
        ...queryFnData,
        initialPlaceholderData: undefined,
      } as Partial<TQueryFnData & { initialPlaceholderData: TPlaceholderData }>;
    },
    buildPlaceholderDataReturn: (placeholderData: TPlaceholderData) => {
      return {
        initialPlaceholderData: placeholderData,
      };
    },
  };
}
