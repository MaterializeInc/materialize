// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import {
  escapedLiteral as lit,
  executeSqlV2,
  rawLimit,
} from "~/api/materialize";

import { queryBuilder } from "../db";

export type SinkErrorsQueryParams = {
  limit?: number;
  sinkId: string;
  startTime: Date;
  endTime: Date;
};

export function buildSinkErrorsQuery({
  sinkId,
  startTime,
  endTime,
  limit = 20,
}: SinkErrorsQueryParams) {
  const qb = queryBuilder
    .selectFrom("mz_sink_status_history as h")
    .select((eb) => eb.fn.max("h.occurred_at").as("lastOccurred"))
    .select("h.error")
    .select((eb) => eb.fn.count<bigint>("h.occurred_at").as("count"))
    .where("h.sink_id", "=", sinkId)
    .where("h.error", "is not", null)
    .$narrowType<{ error: string }>()
    .where(
      sql<boolean>`h.occurred_at between ${lit(
        startTime.toISOString(),
      )} AND ${lit(endTime.toISOString())}`,
    )
    .groupBy("h.error")
    .orderBy("lastOccurred", "desc");

  return rawLimit(qb, limit);
}

export type GroupedError = InferResult<
  ReturnType<typeof buildSinkErrorsQuery>
>[0];

export function fetchSinkErrors(
  queryKey: QueryKey,
  params: SinkErrorsQueryParams,
  requestOptions?: RequestInit,
) {
  const compiledQuery = buildSinkErrorsQuery(params)?.compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type BucketedSinkErrorsQueryParams = {
  sinkId: string;
  startTime: Date;
  endTime: Date;
  bucketSizeSeconds: number;
};

/**
 * Errors for a specific sink, bucketed into intervals for graphing.
 */
export function buildBucketedSinkErrorsQuery({
  sinkId,
  startTime,
  endTime,
  bucketSizeSeconds,
}: BucketedSinkErrorsQueryParams) {
  return queryBuilder
    .selectFrom("mz_sink_status_history as ssh")
    .select((eb) => [
      eb.fn.count<bigint>("ssh.error").as("count"),
      sql<number>`extract(epoch from date_bin(interval ${lit(
        bucketSizeSeconds.toString() + " seconds",
      )}, occurred_at, ${lit(startTime.toISOString())})) * 1000`.as(
        "timestamp",
      ),
    ])
    .where("ssh.sink_id", "=", sinkId)
    .where((eb) => eb.between("ssh.occurred_at", startTime, endTime))
    .groupBy("timestamp")
    .orderBy("timestamp");
}

/**
 * Returns sink errors buckted for graphing
 */
export function fetchBucketedSinkErrors(
  queryKey: QueryKey,
  params: BucketedSinkErrorsQueryParams,
  requestOptions?: RequestInit,
) {
  const compiledQuery = buildBucketedSinkErrorsQuery(params).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
