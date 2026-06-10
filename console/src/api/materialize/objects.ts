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

import { queryBuilder } from "./db";
import { executeSqlV2 } from "./executeSqlV2";
import { getOwners } from "./expressionBuilders";

const ALLOWED_OBJECT_TYPES = [
  "connection",
  "index",
  "materialized-view",
  "secret",
  "sink",
  "source",
  "table",
  "view",
];

export function buildAllObjectsQuery() {
  return queryBuilder
    .selectFrom("mz_object_fully_qualified_names as ofqn")
    .innerJoin("mz_objects as o", "o.id", "ofqn.id")
    .leftJoin("mz_sources as s", "s.id", "ofqn.id")
    .leftJoin("mz_webhook_sources as ws", "ws.id", "ofqn.id")
    .leftJoin("mz_clusters as mc", "mc.id", "ofqn.cluster_id")
    .select([
      "ofqn.id",
      "ofqn.name",
      "ofqn.object_type as objectType",
      "ofqn.schema_id as schemaId",
      "ofqn.schema_name as schemaName",
      "ofqn.database_id as databaseId",
      "ofqn.database_name as databaseName",
      "s.type as sourceType",
      sql<string | null>`
      CASE
        WHEN
          ws.id IS NOT NULL
        THEN true ELSE false
      END
      `.as("isWebhookTable"),
      "o.cluster_id as clusterId",
      "mc.name as clusterName",
    ])
    .where("ofqn.object_type", "in", ALLOWED_OBJECT_TYPES);
}

export type DatabaseObject = InferResult<
  ReturnType<typeof buildAllObjectsQuery>
>[0];

export type IsOwnerParameters = {
  objectId: string;
};

export function buildIsOwnerQuery(params: IsOwnerParameters) {
  return queryBuilder
    .selectFrom("mz_objects as o")
    .innerJoin(getOwners().as("owners"), "owners.id", "o.owner_id")
    .where("o.id", "=", params.objectId)
    .select("owners.isOwner");
}

export function fetchIsOwner({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: IsOwnerParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildIsOwnerQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
