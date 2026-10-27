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

import { executeSqlV2, queryBuilder } from "../";

export type ListFilters = {
  clusterId?: string;
  databaseId?: string;
  nameFilter?: string;
  schemaId?: string;
  includeSystemObjects: boolean;
};

export function buildIndexesListQuery({
  clusterId,
  databaseId,
  schemaId,
  nameFilter,
  includeSystemObjects,
}: ListFilters & { clusterId: string }) {
  let qb = queryBuilder
    .selectFrom("mz_indexes as i")
    .innerJoin("mz_relations as r", "r.id", "i.on_id")
    // We can use the relation's schema id since relations must share the same database and schema as its index
    .innerJoin("mz_schemas as sc", "sc.id", "r.schema_id")
    // System indexes don't belong to a database
    .leftJoin("mz_databases as d", "d.id", "sc.database_id")
    .innerJoin("mz_show_indexes as si", (join) =>
      join.onRef("i.name", "=", "si.name").onRef("si.schema_id", "=", "sc.id"),
    )
    .select([
      "i.id",
      "d.name as databaseName",
      "sc.name as schemaName",
      "i.name",
      "si.key as indexedColumns",
      "r.name as relationName",
      "r.type as relationType",
    ])
    .where("i.cluster_id", "=", clusterId)
    .orderBy(["d.name", "sc.name", "name"]);
  if (databaseId) {
    qb = qb.where("d.id", "=", databaseId);
  }
  if (schemaId) {
    qb = qb.where("sc.id", "=", schemaId);
  }
  if (nameFilter) {
    qb = qb.where("i.name", "like", `%${nameFilter}%`);
  }
  if (!includeSystemObjects) {
    qb = qb.where("i.id", "like", "u%");
  }

  return qb;
}

/**
 * Fetches all indexes for a given cluster
 */
export async function fetchIndexesList({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ListFilters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  if (!filters.clusterId) return null;

  const compiledQuery = buildIndexesListQuery({
    ...filters,
    // narrow the type
    clusterId: filters.clusterId,
  }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type Index = InferResult<ReturnType<typeof buildIndexesListQuery>>[0];
