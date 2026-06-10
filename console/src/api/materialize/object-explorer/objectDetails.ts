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

import { NULL_DATABASE_NAME } from "~/platform/object-explorer/constants";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

export type ObjectExplorerDetailsParameters = {
  databaseName: string;
  schemaName: string;
  name: string;
};

export function buildObjectDetailsQuery(params: {
  databaseName: string;
  schemaName: string;
  name: string;
}) {
  let query = queryBuilder
    .selectFrom("mz_objects as o")
    .innerJoin("mz_object_fully_qualified_names as ofqn", "o.id", "ofqn.id")
    // System schemas don't have a database
    .leftJoin("mz_object_lifetimes as ol", (join) =>
      join
        .onRef("ol.id", "=", "o.id")
        .onRef("ol.object_type", "=", "o.type")
        .on("ol.event_type", "=", "create"),
    )
    .leftJoin("mz_sources as s", "s.id", "o.id")
    .innerJoin("mz_roles as r", "r.id", "o.owner_id")
    .leftJoin("mz_clusters as c", "c.id", "o.cluster_id")
    .select([
      "o.id",
      "o.name",
      // TODO: (#3400) Remove once all subsources are tables
      sql<string>`
      CASE
        WHEN s.type = 'subsource' OR s.type = 'webhook'
        THEN 'table' ELSE o.type
      END
      `.as("type"),
      sql<boolean>`
      CASE
        WHEN s.type = 'subsource' OR
        s.type = 'webhook'
        THEN true
        ELSE false
      END
      `.as("isSourceTable"),
      "ofqn.schema_name as schemaName",
      "ofqn.database_name as databaseName",
      "r.name as owner",
      "ol.occurred_at as createdAt",
      "c.id as clusterId",
      "c.name as clusterName",
    ])
    .where("ofqn.name", "=", params.name)
    .where("ofqn.schema_name", "=", params.schemaName);

  if (params.databaseName === NULL_DATABASE_NAME) {
    query = query.where("ofqn.database_name", "is", null);
  } else {
    query = query.where("ofqn.database_name", "=", params.databaseName);
  }
  return query;
}

export function fetchObjectDetails({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: ObjectExplorerDetailsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectDetailsQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type DatabaseObjectDetails = InferResult<
  ReturnType<typeof buildObjectDetailsQuery>
>[0];
