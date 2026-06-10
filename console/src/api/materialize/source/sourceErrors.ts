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
  queryBuilder,
} from "~/api/materialize";

export type SourceErrorsQueryParams = {
  limit?: number;
  sourceId: string;
  startTime: Date;
  endTime: Date;
};

/**
 * Errors for a specific source and its tables
 */
export function buildSourceErrorsQuery({
  limit = 20,
  sourceId,
  startTime,
  endTime,
}: SourceErrorsQueryParams) {
  return queryBuilder
    .selectFrom("mz_source_status_history as h")
    .leftJoin("mz_object_dependencies as od", "h.source_id", "od.object_id")
    .select((eb) => eb.fn.max("h.occurred_at").as("lastOccurred"))
    .select("h.error")
    .select((eb) => eb.fn.count<bigint>("h.occurred_at").as("count"))
    .where((eb) =>
      eb.or([
        eb("od.referenced_object_id", "=", sourceId),
        eb("h.source_id", "=", sourceId),
      ]),
    )
    .where("error", "is not", null)
    .where((eb) => eb.between("h.occurred_at", startTime, endTime))
    .groupBy("h.error")
    .orderBy("lastOccurred", "desc")
    .limit((eb) => eb.lit(limit));
}

export type GroupedError = InferResult<
  ReturnType<typeof buildSourceErrorsQuery>
>[0];

export function fetchSourceErrors(
  queryKey: QueryKey,
  params: SourceErrorsQueryParams,
  requestOptions?: RequestInit,
) {
  const compiledQuery = buildSourceErrorsQuery(params)?.compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type BucketedSourceErrorsQueryParams = {
  sourceId: string;
  startTime: Date;
  endTime: Date;
  bucketSizeSeconds: number;
};

/**
 * Errors for a specific source and its tables, bucketed into intervals for graphing.
 */
export function buildBucketedSourceErrorsQuery({
  sourceId,
  startTime,
  endTime,
  bucketSizeSeconds,
}: BucketedSourceErrorsQueryParams) {
  return queryBuilder
    .selectFrom("mz_source_status_history as ssh")
    .leftJoin("mz_object_dependencies as od", "od.object_id", "ssh.source_id")
    .select((eb) => [
      eb.fn.count<bigint>("ssh.error").as("count"),
      sql<number>`extract(epoch from date_bin(interval ${lit(
        bucketSizeSeconds.toString() + " seconds",
      )}, occurred_at, ${lit(startTime.toISOString())})) * 1000`.as(
        "timestamp",
      ),
    ])
    .where((eb) =>
      eb.or([
        eb("od.referenced_object_id", "=", sourceId),
        eb("ssh.source_id", "=", sourceId),
      ]),
    )
    .where((eb) => eb.between("ssh.occurred_at", startTime, endTime))
    .groupBy("timestamp")
    .orderBy("timestamp");
}

export function fetchBucketedSourceErrors(
  queryKey: QueryKey,
  params: BucketedSourceErrorsQueryParams,
  requestOptions?: RequestInit,
) {
  const compiledQuery = buildBucketedSourceErrorsQuery(params)?.compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
