// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { executeSqlV2, queryBuilder } from "..";

type NonRunningSourcesQueryParams = {
  objectIds: string[];
};
/**
 * Given a list of object ids, returns all transitive 'non-running' sources and additional information
 *
 */
export function buildNonRunningSourcesQuery({
  objectIds,
}: NonRunningSourcesQueryParams) {
  return queryBuilder
    .selectFrom("mz_sources as sources")
    .innerJoin("mz_source_statuses as ss", "ss.id", "sources.id")
    .innerJoin(
      "mz_object_fully_qualified_names as names",
      "sources.id",
      "names.id",
    )
    .where("sources.type", "<>", "subsource")
    .where("sources.type", "<>", "progress")
    .where("ss.status", "!=", "running")
    .where((eb) =>
      eb.or([
        eb("sources.id", "in", objectIds),
        // Get sources that are upstream of the current object ids
        eb("sources.id", "in", (qb) =>
          qb
            .selectFrom("mz_object_transitive_dependencies as deps")
            .where("deps.object_id", "in", objectIds)
            .select("deps.referenced_object_id"),
        ),
      ]),
    )
    .select([
      "ss.id",
      "ss.name",
      "ss.status",
      "names.schema_name as schemaName",
      "names.database_name as databaseName",
    ]);
}

export type NonRunningSourceInfo = InferResult<
  ReturnType<typeof buildNonRunningSourcesQuery>
>[0];

/**
 * Given a list of object ids, returns if they're sources and are not running
 *
 */
export async function fetchNonRunningSources(
  filters: NonRunningSourcesQueryParams,
  queryKey: QueryKey,
  requestOptions: RequestInit,
) {
  const compiledQuery = buildNonRunningSourcesQuery(filters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
