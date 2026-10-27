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

import { NULL_DATABASE_NAME } from "~/platform/object-explorer/constants";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";
import { getOwners } from "../expressionBuilders";

export type SchemaDetailsParameters = {
  databaseName: string;
  name: string;
};

export function buildSchemaDetailsQuery(params: SchemaDetailsParameters) {
  let query = queryBuilder
    .selectFrom("mz_schemas as s")
    // System schemas don't have a database
    .leftJoin("mz_databases as d", "d.id", "s.database_id")
    // Due to an ID migration, some schemas won't have a matching create record
    .leftJoin("mz_object_lifetimes as ol", (join) =>
      join
        .onRef("ol.id", "=", "s.id")
        .on("ol.object_type", "=", "schema")
        .on("ol.event_type", "=", "create"),
    )
    .innerJoin("mz_roles as r", "r.id", "s.owner_id")
    .innerJoin(getOwners().as("owners"), "owners.id", "s.owner_id")
    .select([
      "s.id",
      "s.name",
      "d.name as databaseName",
      "ol.occurred_at as createdAt",
      "owners.isOwner",
      "owners.name as owner",
    ])
    .where("s.name", "=", params.name);

  if (params.databaseName === NULL_DATABASE_NAME) {
    query = query.where("d.name", "is", null);
  } else {
    query = query.where("d.name", "=", params.databaseName);
  }
  return query;
}

export function fetchSchemaDetails({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: SchemaDetailsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildSchemaDetailsQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type SchemaDetails = InferResult<
  ReturnType<typeof buildSchemaDetailsQuery>
>[0];
