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

import { queryBuilder } from "~/api/materialize/db";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import { getOwners } from "~/api/materialize/expressionBuilders";

export type ListFilters = {
  databaseId?: string;
  schemaId?: string;
  nameFilter?: string;
};

export function buildSecretsListQuery({
  databaseId,
  schemaId,
  nameFilter,
}: ListFilters) {
  let qb = queryBuilder
    .selectFrom("mz_secrets as s")
    .innerJoin("mz_schemas as sc", "sc.id", "s.schema_id")
    .innerJoin("mz_databases as d", "d.id", "sc.database_id")
    .innerJoin(getOwners().as("owners"), "owners.id", "s.owner_id")
    .innerJoin("mz_object_lifetimes as ol", (join) =>
      join
        .onRef("ol.id", "=", "s.id")
        .on("ol.event_type", "=", "create")
        .on("ol.object_type", "=", "secret"),
    )
    .select([
      "s.id",
      "s.name",
      "ol.occurred_at as createdAt",
      "d.name as databaseName",
      "sc.name as schemaName",
      "owners.isOwner",
    ])
    .where("s.id", "like", "u%")
    .orderBy(["d.name", "sc.name", "name"]);
  if (databaseId) {
    qb = qb.where("d.id", "=", databaseId);
  }
  if (schemaId) {
    qb = qb.where("sc.id", "=", schemaId);
  }
  if (nameFilter) {
    qb = qb.where("s.name", "like", `%${nameFilter}%`);
  }
  return qb;
}

/**
 * Fetches all secrets in the current environment
 */
export async function fetchSecretsList({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ListFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildSecretsListQuery(filters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type ListPageSecret = InferResult<
  ReturnType<typeof buildSecretsListQuery>
>[0];
