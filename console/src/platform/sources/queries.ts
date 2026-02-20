// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  useMutation,
  useQueryClient,
  useSuspenseQuery,
} from "@tanstack/react-query";
import { subMinutes } from "date-fns";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
  extractEnvironmentVersion,
} from "~/api/buildQueryKeySchema";
import createWebhookSource, {
  CreateWebhookSourceParameters,
} from "~/api/materialize/source/createWebhookSourceStatement";
import createWebhookSourceView, {
  CreateWebhookSourceViewParams,
} from "~/api/materialize/source/createWebhookSourceView";
import {
  fetchBucketedSourceErrors,
  fetchSourceErrors,
} from "~/api/materialize/source/sourceErrors";
import {
  fetchSourceList,
  ListFilters,
} from "~/api/materialize/source/sourceList";
import {
  buildSourceStatisticsQuery,
  COLLECTION_INTERVAL_MS,
  fetchSourceStatistics,
  SourceStatisticsDataPoint,
} from "~/api/materialize/source/sourceStatistics";
import {
  fetchSourceTables,
  SourceTableParams,
} from "~/api/materialize/source/sourceTables";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useSubscribeManager,
} from "~/api/materialize/useSubscribe";

export type SourceListQueryKeyParams = {
  filters?: ListFilters;
};

export type SourceStatisticsQueryKeyParams = {
  sourceId: string;
};

export const sourceQueryKeys = {
  all: () => buildRegionQueryKey("source"),
  list: (params?: SourceListQueryKeyParams) =>
    [
      ...sourceQueryKeys.all(),
      buildQueryKeyPart(
        "list",
        params
          ? {
              filters: params.filters,
            }
          : undefined,
      ),
    ] as const,
  show: (params: { sourceId: string }) =>
    [...sourceQueryKeys.all(), buildQueryKeyPart("show", params)] as const,
  errors: (params: SourceErrorsQueryKeyParams) =>
    [...sourceQueryKeys.all(), buildQueryKeyPart("errors", params)] as const,
  statistics: (params: SourceStatisticsQueryKeyParams) =>
    [
      ...sourceQueryKeys.all(),
      buildQueryKeyPart("statistics", {
        sourceId: params.sourceId,
      }),
    ] as const,
  bucketedErrors: (params: BucketedSourceErrorsQueryKeyParams) =>
    [
      ...sourceQueryKeys.all(),
      buildQueryKeyPart("bucketedErrors", params),
    ] as const,
  webhookSources: () => [
    ...sourceQueryKeys.all(),
    buildQueryKeyPart("webhooks"),
  ],
  createWebhookSource: () => [
    ...sourceQueryKeys.webhookSources(),
    buildQueryKeyPart("create"),
  ],
  createWebhookSourceView: () => [
    ...sourceQueryKeys.webhookSources(),
    buildQueryKeyPart("createView"),
  ],
  sourceTables: (params: { sourceId: string }) =>
    [
      ...sourceQueryKeys.all(),
      buildQueryKeyPart("sourceTables", params),
    ] as const,
};

export function useSourcesList(filters: ListFilters = {}) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sourceQueryKeys.list({ filters }),
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchSourceList({
        queryKey,
        parameters: {
          filters: queryKeyParams.filters ?? {},
        },
        requestOptions: { signal },
      });
    },
  });
}

export type SourceErrorsQueryKeyParams = {
  sourceId: string;
  timePeriodMinutes: number;
};

/**
 * Fetches errors for a specific source and its tables
 */
export function useSourceErrors(params: SourceErrorsQueryKeyParams) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sourceQueryKeys.errors(params),
    queryFn: ({ queryKey, signal }) => {
      const [, paramsQueryKeyPart] = queryKey;
      const endTime = new Date();
      const startTime = subMinutes(
        endTime,
        paramsQueryKeyPart.timePeriodMinutes,
      );
      return fetchSourceErrors(
        queryKey,
        {
          sourceId: paramsQueryKeyPart.sourceId,
          startTime,
          endTime,
        },
        { signal },
      );
    },
  });
}

export type BucketedSourceErrorsQueryKeyParams = {
  sourceId: string;
  timePeriodMinutes: number;
  bucketSizeSeconds: number;
};

/**
 * Fetches errors for a specific source and its tables
 */
export function useBucketedSourceErrors(
  params: BucketedSourceErrorsQueryKeyParams,
) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sourceQueryKeys.bucketedErrors(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, paramsQueryKeyPart] = queryKey;
      const endTime = new Date();
      const startTime = subMinutes(
        endTime,
        paramsQueryKeyPart.timePeriodMinutes,
      );
      const result = await fetchBucketedSourceErrors(
        queryKey,
        {
          sourceId: paramsQueryKeyPart.sourceId,
          bucketSizeSeconds: paramsQueryKeyPart.bucketSizeSeconds,
          startTime,
          endTime,
        },
        { signal },
      );
      return {
        startTime,
        endTime,
        ...result,
      };
    },
    select: (data) => ({
      ...data,
      rows: data.rows.map((r) => ({ ...r, count: Number(r.count) })),
    }),
  });
}

export const useCurrentSourceStatistics = (params: { sourceId: string }) => {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sourceQueryKeys.statistics(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchSourceStatistics(
        queryKey,
        {
          sourceId: queryKeyParams.sourceId,
        },
        { signal },
      );
    },
  });
};

export const SOURCE_STATISTICS_TIME_PERIOD_PADDING =
  (COLLECTION_INTERVAL_MS / 1000 / 60) * 2;

export const useSourceStatistics = ({
  sourceId,
  timePeriodMinutes,
}: {
  sourceId: string;
  timePeriodMinutes: number;
}) => {
  const [currentTimePeriod, setCurrentTimePeriod] = React.useState(0);
  const [initialStartTime, setInitialStartTime] = React.useState<
    Date | undefined
  >();
  const [initialEndTime, setInitialEndTime] = React.useState(new Date());

  const timePeriodWithPadding =
    timePeriodMinutes + SOURCE_STATISTICS_TIME_PERIOD_PADDING;

  // Extract environment version from the query key
  const queryKey = sourceQueryKeys.statistics({ sourceId });
  const environmentVersion = extractEnvironmentVersion(queryKey);

  const subscribe = React.useMemo(() => {
    if (!initialStartTime) return undefined;

    // Pass the environment version to buildSourceStatisticsQuery
    return buildSubscribeQuery(
      buildSourceStatisticsQuery(sourceId, environmentVersion),
      {
        asOfAtLeast: initialStartTime,
        upsertKey: "id",
      },
    );
  }, [initialStartTime, sourceId, environmentVersion]);

  const { data, disconnect, ...rest } =
    useSubscribeManager<SourceStatisticsDataPoint>({ subscribe });

  // When the time period is set, we pick the current time as the end time for the graph,
  // then work backwards to calculate the initialStartTime based on the selected time period.
  if (currentTimePeriod !== timePeriodWithPadding) {
    const newEndTime = new Date();
    setCurrentTimePeriod(timePeriodWithPadding);
    setInitialEndTime(newEndTime);
    setInitialStartTime(subMinutes(newEndTime, timePeriodWithPadding));
    disconnect();
  }

  // This is the current end time of the graph
  const currentEndTime = React.useMemo(() => {
    if (data.length > 1) {
      const newestData = new Date(data.at(-1)?.mzTimestamp ?? 0);
      if (initialEndTime < newestData) {
        return newestData;
      }
    }
    return initialEndTime;
  }, [data, initialEndTime]);

  // This is the current start time of the graph
  const currentStartTime = React.useMemo(() => {
    return subMinutes(currentEndTime, timePeriodMinutes);
  }, [currentEndTime, timePeriodMinutes]);

  const paddedStartTime = React.useMemo(() => {
    // Add padding so we have data to calculate rates for the initial bucket
    return subMinutes(currentEndTime, timePeriodWithPadding);
  }, [currentEndTime, timePeriodWithPadding]);

  return {
    data,
    disconnect,
    /** Start time of the graph, based on the select time period */
    currentStartTime,
    currentEndTime,
    /** Start time of the data, padded to allow rate calculations */
    paddedStartTime,
    ...rest,
  };
};

export type SourceStatisticsRow = SubscribeRow<SourceStatisticsDataPoint>;

export type SourceListResponse = ReturnType<typeof useSourcesList>;

export function useCreateWebhookSource() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: sourceQueryKeys.createWebhookSource(),
    mutationFn: (params: CreateWebhookSourceParameters) =>
      createWebhookSource({
        params,
        queryKey: sourceQueryKeys.createWebhookSource(),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: sourceQueryKeys.list(),
      });
    },
  });
}

export function useCreateWebhookSourceView() {
  return useMutation({
    mutationKey: sourceQueryKeys.createWebhookSourceView(),
    mutationFn: (params: CreateWebhookSourceViewParams) =>
      createWebhookSourceView({
        params,
        queryKey: sourceQueryKeys.createWebhookSourceView(),
      }),
  });
}

export function useSourceTables(params: SourceTableParams) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sourceQueryKeys.sourceTables(params),
    queryFn: ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      return fetchSourceTables({
        queryKey,
        params: paramsFromKey,
        requestOptions: { signal },
      });
    },
  });
}
