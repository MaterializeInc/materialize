// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";
import { subMinutes } from "date-fns";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
  extractEnvironmentVersion,
} from "~/api/buildQueryKeySchema";
import {
  fetchBucketedSinkErrors,
  fetchSinkErrors,
} from "~/api/materialize/sink/sinkErrors";
import { fetchSinkList, ListFilters } from "~/api/materialize/sink/sinkList";
import {
  buildSinkStatisticsQuery,
  COLLECTION_INTERVAL_MS,
  fetchSinkStatistics,
  SinkStatisticsDataPoint,
} from "~/api/materialize/sink/sinkStatistics";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useSubscribeManager,
} from "~/api/materialize/useSubscribe";

export type SinkErrorsQueryKeyParams = {
  sinkId: string;
  timePeriodMinutes: number;
};

export type SinkStatisticsQueryKeyParams = {
  sinkId: string;
};

export const sinkQueryKeys = {
  all: () => buildRegionQueryKey("sink"),
  list: (filters?: ListFilters) =>
    [...sinkQueryKeys.all(), buildQueryKeyPart("list", filters)] as const,
  errors: (params: SinkErrorsQueryKeyParams) =>
    [...sinkQueryKeys.all(), buildQueryKeyPart("errors", params)] as const,
  bucketedErrors: (params: BucketedSinkErrorsQueryKeyParams) =>
    [
      ...sinkQueryKeys.all(),
      buildQueryKeyPart("bucketedErrors", params),
    ] as const,
  statistics: (params: SinkStatisticsQueryKeyParams) =>
    [
      ...sinkQueryKeys.all(),
      buildQueryKeyPart("statistics", {
        sinkId: params.sinkId,
      }),
    ] as const,
};

/**
 * Fetches a list of sinks, optionally restricted to the provided filters.
 */
export function useSinkList(filters: ListFilters = {}) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sinkQueryKeys.list(filters),
    queryFn: ({ queryKey, signal }) => {
      return fetchSinkList({
        queryKey,
        filters,
        requestOptions: { signal },
      });
    },
  });
}

/**
 * Fetches errors for a specific sink
 */
export function useSinkErrors(params: SinkErrorsQueryKeyParams) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sinkQueryKeys.errors(params),
    queryFn: ({ queryKey, signal }) => {
      const [, paramsQueryKeyPart] = queryKey;
      const endTime = new Date();
      const startTime = subMinutes(
        endTime,
        paramsQueryKeyPart.timePeriodMinutes,
      );
      return fetchSinkErrors(
        queryKey,
        {
          sinkId: paramsQueryKeyPart.sinkId,
          startTime,
          endTime,
        },
        { signal },
      );
    },
  });
}

export type BucketedSinkErrorsQueryKeyParams = {
  sinkId: string;
  timePeriodMinutes: number;
  bucketSizeSeconds: number;
};

/**
 * Fetches errors for a specific sinks
 */
export function useBucketedSinkErrors(
  params: BucketedSinkErrorsQueryKeyParams,
) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sinkQueryKeys.bucketedErrors(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, paramsQueryKeyPart] = queryKey;
      const endTime = new Date();
      const startTime = subMinutes(
        endTime,
        paramsQueryKeyPart.timePeriodMinutes,
      );
      const result = await fetchBucketedSinkErrors(
        queryKey,
        {
          sinkId: paramsQueryKeyPart.sinkId,
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

export const useCurrentSinkStatistics = (params: { sinkId: string }) => {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: sinkQueryKeys.statistics(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchSinkStatistics(
        queryKey,
        {
          sinkId: queryKeyParams.sinkId,
        },
        { signal },
      );
    },
  });
};

export const SINK_STATISTICS_TIME_PERIOD_PADDING =
  (COLLECTION_INTERVAL_MS / 1000 / 60) * 2;

export const useSinkStatistics = ({
  sinkId,
  timePeriodMinutes,
}: {
  sinkId: string;
  timePeriodMinutes: number;
}) => {
  const [currentTimePeriod, setCurrentTimePeriod] = React.useState(0);
  const [initialStartTime, setInitialStartTime] = React.useState<
    Date | undefined
  >();
  const [initialEndTime, setInitialEndTime] = React.useState(new Date());

  const timePeriodWithPadding =
    timePeriodMinutes + SINK_STATISTICS_TIME_PERIOD_PADDING;

  // Extract environment version from the query key
  const queryKey = sinkQueryKeys.statistics({ sinkId });
  const environmentVersion = extractEnvironmentVersion(queryKey);

  const subscribe = React.useMemo(() => {
    if (!initialStartTime) return undefined;

    return buildSubscribeQuery(
      buildSinkStatisticsQuery(sinkId, environmentVersion),
      {
        asOfAtLeast: initialStartTime,
        upsertKey: "id",
      },
    );
  }, [initialStartTime, sinkId, environmentVersion]);

  const { data, disconnect, ...rest } =
    useSubscribeManager<SinkStatisticsDataPoint>({ subscribe });

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

export type SinkStatisticsRow = SubscribeRow<SinkStatisticsDataPoint>;
